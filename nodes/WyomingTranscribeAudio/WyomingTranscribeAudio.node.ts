import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import * as net from 'net';
import * as path from 'path';
import { spawn } from 'child_process'; // Import spawn for ffmpeg
import { performance } from 'perf_hooks'; // Import for high-resolution timing

// --- Constants ---
const __version__ = '1.0.0'; // Wyoming protocol version compatibility
const SAMPLE_RATE = 16000; // Target sample rate for Wyoming
const SAMPLE_WIDTH = 2; // Target sample width (bytes) for Wyoming (16-bit)
const SAMPLE_CHANNELS = 1; // Target channels for Wyoming (Mono)
const NODE_NAME_LOG_PREFIX = '[WyomingTranscribeAudio]';
const NEWLINE = Buffer.from('\n', 'utf-8');
const WYOMING_AUDIO_FORMAT_DESC = 'PCM, 16kHz sample rate, 16-bit depth, Mono channel';

// Utility to yield the event loop, useful for async operations in tight loops or socket handling
const yieldEventLoop = () => new Promise(resolve => setImmediate(resolve));

// --- Standalone Helper Functions ---

/**
 * Sends a Wyoming event formatted according to wyoming/event.py protocol.
 */
async function _sendWyomingEventRevised(
	client: net.Socket,
	eventType: string,
	eventData: object | null,
	payload: Buffer | null = null,
	logger: IExecuteFunctions['logger'],
	logPrefix: string
): Promise<boolean> {
	let overallSuccess = true;
	try {
		const primaryJson: Record<string, any> = { type: eventType, version: __version__ };
		let dataBytes: Buffer | null = null;
		if (eventData && Object.keys(eventData).length > 0) {
			try {
				dataBytes = Buffer.from(JSON.stringify(eventData), 'utf-8');
				primaryJson['data_length'] = dataBytes.length;
			} catch(e: any) {
				logger.error(`${logPrefix} Failed to stringify event data for type ${eventType}: ${e.message}`);
				throw e;
			}
		}
		if (payload) {
			primaryJson['payload_length'] = payload.length;
		}
		const primaryJsonString = JSON.stringify(primaryJson);
		const primaryJsonBuffer = Buffer.from(primaryJsonString, 'utf-8');
		logger.debug(`${logPrefix} Sending event: Type=${eventType}, DataLength=${primaryJson.data_length ?? 0}, PayloadLength=${primaryJson.payload_length ?? 0}`);
		await yieldEventLoop();
		let writeSuccess = client.write(primaryJsonBuffer);
		writeSuccess &&= client.write(NEWLINE);
		overallSuccess &&= writeSuccess;
		if (dataBytes) {
			const dataWriteSuccess = client.write(dataBytes);
			if (!dataWriteSuccess && overallSuccess) logger.warn(`${logPrefix} Write buffer full after Data Bytes for ${eventType}.`);
			overallSuccess &&= dataWriteSuccess;
		}
		if (payload) {
			const payloadWriteSuccess = client.write(payload);
			if (!payloadWriteSuccess && overallSuccess) logger.warn(`${logPrefix} Write buffer full after Payload Bytes for ${eventType}.`);
			overallSuccess &&= payloadWriteSuccess;
		}
	} catch (error: any) {
		logger.error(`${logPrefix} Error during event construction/write for event ${eventType}: ${error.message}`, error);
		await yieldEventLoop();
		return false;
	}
	return overallSuccess;
}

/**
 * Handles the audio transcription process via Wyoming protocol, assuming correctly formatted PCM input.
 * Adds flags to rejected errors for specific failure types.
 */
async function _transcribeAudio(
	execContext: IExecuteFunctions,
	itemIndex: number,
	serverAddress: string,
	audioBuffer: Buffer,
	language: string,
	timeoutMs: number,
): Promise<string> {
	const logger = execContext.logger;
	const logPrefix = `${NODE_NAME_LOG_PREFIX} Item ${itemIndex}:`;

	return new Promise(async (resolve, reject) => {
		let host: string;
		let port: number;
		try {
			const parts = serverAddress.split(':');
			if (parts.length !== 2 || !parts[0] || !parts[1] || isNaN(parseInt(parts[1], 10))) { throw new NodeOperationError(execContext.getNode(), 'Invalid server address format. Use host:port'); }
			host = parts[0]; port = parseInt(parts[1], 10);
			if (port <= 0 || port > 65535) { throw new NodeOperationError(execContext.getNode(), 'Invalid port number.'); }
		} catch (error: any) {
			logger.error(`${logPrefix} Error parsing server address: ${error.message}`);
			const addrError = new NodeOperationError(execContext.getNode(), `Invalid Wyoming Server Address: ${error.message}`, { itemIndex });
			(addrError as any).isConfigurationError = true; // Flag config error
			return reject(addrError);
		}

		const client = new net.Socket();
		let receivedDataBuffer = Buffer.alloc(0);
		let transcriptReceived = false;
		let connectionClosed = false;
		let appTimeoutHandle: NodeJS.Timeout | null = null;

		const cleanup = async (reason?: string) => {
			if (appTimeoutHandle) { clearTimeout(appTimeoutHandle); appTimeoutHandle = null; }
			if (!connectionClosed) {
				connectionClosed = true;
				if (!client.destroyed) {
					logger.debug(`${logPrefix} Cleaning up connection (${reason || 'normal close'}). State: ${client.readyState}`);
					await yieldEventLoop();
					client.removeAllListeners(); client.end(); client.destroySoon();
				}
			}
		};

		appTimeoutHandle = setTimeout(async () => {
			const errorMsg = `Application timeout reached after ${timeoutMs}ms waiting for transcription`;
			logger.warn(`${logPrefix} ${errorMsg}`);
			await cleanup('application timeout');
			if (!transcriptReceived && !connectionClosed) {
				const timeoutError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				(timeoutError as any).isTimeout = true; // Flag timeout error
				reject(timeoutError);
			}
		}, timeoutMs);

		client.on('connect', async () => {
			logger.info(`${logPrefix} TCP connection established with ${host}:${port}.`);
			await yieldEventLoop();
			try {
				let overallWriteSuccess = true;
				overallWriteSuccess &&= await _sendWyomingEventRevised(client, 'transcribe', { language }, null, logger, logPrefix);
				overallWriteSuccess &&= await _sendWyomingEventRevised(client, 'audio-start', { rate: SAMPLE_RATE, width: SAMPLE_WIDTH, channels: SAMPLE_CHANNELS }, null, logger, logPrefix);
				overallWriteSuccess &&= await _sendWyomingEventRevised(client, 'audio-chunk', { rate: SAMPLE_RATE, width: SAMPLE_WIDTH, channels: SAMPLE_CHANNELS }, audioBuffer, logger, logPrefix);
				overallWriteSuccess &&= await _sendWyomingEventRevised(client, 'audio-stop', {}, null, logger, logPrefix);
				if (!overallWriteSuccess) logger.warn(`${logPrefix} Completed sending sequence, but kernel buffer reported full at some point.`);
				else logger.debug(`${logPrefix} Successfully sent transcription request sequence.`);
				await yieldEventLoop();
			} catch (err: any) {
				const errorMsg = `Error during initial data sending sequence: ${err.message}`;
				logger.error(`${logPrefix} ${errorMsg}`, err);
				await cleanup('write error sequence');
				if (appTimeoutHandle) reject(new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex })); // Don't flag specific type here
			}
		});

		client.on('data', async (chunk: Buffer) => {
			receivedDataBuffer = Buffer.concat([receivedDataBuffer, chunk]);
			await yieldEventLoop();
			let requiresCleanup = false;
			let resolveValue: string | undefined;
			let rejectError: Error | undefined;
			while(true) {
				const newlineIndex = receivedDataBuffer.indexOf(NEWLINE);
				if (newlineIndex === -1) break;
				const primaryJsonBuffer = receivedDataBuffer.subarray(0, newlineIndex);
				const primaryJsonString = primaryJsonBuffer.toString('utf-8');
				receivedDataBuffer = receivedDataBuffer.subarray(newlineIndex + NEWLINE.length);
				await yieldEventLoop();
				let eventDict: Record<string, any>;
				try { eventDict = JSON.parse(primaryJsonString); } catch (parseError: any) {
					logger.error(`${logPrefix} Failed to parse Primary JSON Line: "${primaryJsonString}". Error: ${parseError.message}`);
					await yieldEventLoop(); continue;
				}
				const dataLength = eventDict['data_length'] as number | undefined;
				let dataDict: object | null = eventDict['data'] || null;
				let originalDataBytes: Buffer | null = null;
				if (dataLength && dataLength > 0) {
					if (receivedDataBuffer.length < dataLength) { receivedDataBuffer = Buffer.concat([primaryJsonBuffer, NEWLINE, receivedDataBuffer]); break; }
					originalDataBytes = receivedDataBuffer.subarray(0, dataLength);
					receivedDataBuffer = receivedDataBuffer.subarray(dataLength);
					await yieldEventLoop();
					try { const separateDataDict = JSON.parse(originalDataBytes.toString('utf-8')); if (!dataDict) dataDict = {}; Object.assign(dataDict, separateDataDict); } catch (parseError: any) {
						logger.error(`${logPrefix} Failed to parse separate Data Bytes: ${originalDataBytes.toString('utf-8')}. Error: ${parseError.message}`); continue;
					}
				}
				const payloadLength = eventDict['payload_length'] as number | undefined;
				let payloadExists = false;
				if (payloadLength && payloadLength > 0) {
					if (receivedDataBuffer.length < payloadLength) { const rebufferParts = [primaryJsonBuffer, NEWLINE]; if (originalDataBytes) rebufferParts.push(originalDataBytes); rebufferParts.push(receivedDataBuffer); receivedDataBuffer = Buffer.concat(rebufferParts); break; }
					receivedDataBuffer = receivedDataBuffer.subarray(payloadLength);
					payloadExists = true;
					await yieldEventLoop();
				}
				const eventType = eventDict['type'] as string;
				logger.info(`${logPrefix} Rcvd & Parsed Complete Event: Type=${eventType}, Data=${JSON.stringify(dataDict)}, PayloadIncluded=${payloadExists}`);
				await yieldEventLoop();
				if (eventType === 'transcript' && dataDict && typeof (dataDict as any).text === 'string') {
					const transcription = (dataDict as any).text;
					logger.info(`${logPrefix} Transcription successful: "${transcription}"`);
					transcriptReceived = true; requiresCleanup = true; resolveValue = transcription; break;
				} else if (eventType === 'error') {
					const errMsg = (dataDict as any)?.message || 'Unknown server error';
					logger.error(`${logPrefix} Received error event from server: ${errMsg}`);
					rejectError = new NodeOperationError(execContext.getNode(), `Wyoming server error: ${errMsg}`, { itemIndex });
					(rejectError as any).isServerError = true; // Flag server-reported error
					requiresCleanup = true; break;
				} else { logger.warn(`${logPrefix} Received unhandled/ignored event type: ${eventType}`); }
				if (receivedDataBuffer.length === 0) break;
			}
			if (requiresCleanup) {
				await cleanup(resolveValue ? 'transcript received' : 'server error event');
				if (rejectError) reject(rejectError);
				else if (resolveValue !== undefined) resolve(resolveValue);
			}
		});

		client.on('end', async () => {
			await cleanup('server ended connection');
			if (!transcriptReceived && appTimeoutHandle) {
				const errorMsg = 'Connection closed by server before transcript received.';
				logger.warn(`${logPrefix} ${errorMsg}`);
				const closeError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				(closeError as any).isConnectionClosed = true; // Flag premature close
				reject(closeError);
			}
		});

		client.on('close', async (hadError: boolean) => {
			const cleanupReason = `closed${hadError ? ' with error' : ''}`;
			const shouldReject = !transcriptReceived && appTimeoutHandle;
			await cleanup(cleanupReason);
			if (shouldReject) {
				 const errorMsg = `Connection closed unexpectedly${hadError ? ' with error' : ''}.`;
				 logger.warn(`${logPrefix} ${errorMsg}`);
				 const closeError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				 (closeError as any).isConnectionClosed = true; // Flag premature close
				 reject(closeError);
			 }
		});

		client.on('error', async (err: Error & { code?: string }) => { // Add code type hint
			const code = err.code;
			let errorMsg = `Socket error: ${err.message}`;
			if (code) errorMsg += ` (Code: ${code})`;
			logger.error(`${logPrefix} ${errorMsg}`, err);
			await cleanup('socket error');
			if (appTimeoutHandle) {
				const nodeError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				(nodeError as any).originalCode = code; // Preserve original code
				reject(nodeError);
			}
		});

		client.on('timeout', async () => {
			logger.warn(`${logPrefix} Socket inactivity timeout triggered.`);
			await cleanup('socket inactivity timeout');
		});

		client.on('drain', async () => { await yieldEventLoop(); });

		logger.debug(`${logPrefix} Attempting to connect to ${host}:${port}...`);
		await yieldEventLoop();
		client.connect(port, host);
	});
}

/**
 * Converts audio buffer to the target PCM format (s16le, 16kHz, mono) using ffmpeg.
 * Adds flags to rejected errors for specific failure types.
 */
async function convertAudioToPcm(
	inputBuffer: Buffer,
	_inputFileNameHint: string | undefined,
	targetSampleRate: number,
	targetChannels: number,
	logger: IExecuteFunctions['logger'],
	logPrefix: string,
): Promise<Buffer> {

	const ffmpegArgs = ['-loglevel','error','-i','pipe:0','-f','s16le','-ar',String(targetSampleRate),'-ac',String(targetChannels),'-'];
	logger.info(`${logPrefix} Starting audio conversion with ffmpeg. Target: ${targetSampleRate}Hz, ${targetChannels === 1 ? 'Mono' : 'Stereo'}.`);
	logger.debug(`${logPrefix} Running ffmpeg command: ffmpeg ${ffmpegArgs.join(' ')}`);

	return new Promise<Buffer>((resolve, reject) => {
		const ffmpegProcess = spawn('ffmpeg', ffmpegArgs, { stdio: ['pipe', 'pipe', 'pipe'] });
		const outputChunks: Buffer[] = [];
		const errorChunks: Buffer[] = [];
		ffmpegProcess.stdout.on('data', (chunk: Buffer) => outputChunks.push(chunk));
		ffmpegProcess.stderr.on('data', (chunk: Buffer) => errorChunks.push(chunk));

		ffmpegProcess.on('close', (code) => {
			const stderrOutput = Buffer.concat(errorChunks).toString('utf-8').trim();
			if (code !== 0) {
				logger.error(`${logPrefix} ffmpeg process exited with code ${code}.`);
				if (stderrOutput) logger.error(`${logPrefix} ffmpeg stderr: ${stderrOutput}`);
				const ffmpegError = new Error(`ffmpeg conversion failed (exit code ${code})${stderrOutput ? `: ${stderrOutput}` : ''}`);
				(ffmpegError as any).isFFmpegError = true; // Flag ffmpeg execution error
				reject(ffmpegError);
			} else {
				const pcmBuffer = Buffer.concat(outputChunks);
				if (pcmBuffer.length === 0) {
					logger.error(`${logPrefix} ffmpeg conversion succeeded but produced empty output.`);
					if (stderrOutput) logger.warn(`${logPrefix} ffmpeg stderr (may contain clues): ${stderrOutput}`);
					const ffmpegError = new Error('ffmpeg conversion produced empty output.');
					(ffmpegError as any).isFFmpegError = true; // Flag empty output as ffmpeg related
					reject(ffmpegError);
				} else if (pcmBuffer.length % SAMPLE_WIDTH !== 0) {
                    logger.warn(`${logPrefix} Converted PCM buffer length (${pcmBuffer.length} bytes) is not a multiple of sample width (${SAMPLE_WIDTH}).`);
                    if (stderrOutput) logger.warn(`${logPrefix} ffmpeg stderr: ${stderrOutput}`);
					const ffmpegError = new Error(`Converted PCM data length ${pcmBuffer.length} is not multiple of sample width ${SAMPLE_WIDTH}.`);
					(ffmpegError as any).isFFmpegError = true; // Flag bad output as ffmpeg related
					reject(ffmpegError);
				} else {
					logger.info(`${logPrefix} ffmpeg conversion successful. Resulting PCM buffer size: ${pcmBuffer.length} bytes.`);
					if (stderrOutput) logger.warn(`${logPrefix} ffmpeg stderr (possibly warnings): ${stderrOutput}`);
					resolve(pcmBuffer);
				}
			}
		});

		ffmpegProcess.on('error', (err) => { // Handle process spawn errors
			logger.error(`${logPrefix} Failed to start ffmpeg process: ${err.message}`);
			const spawnError = new Error(`Failed to spawn ffmpeg: ${err.message}. Is ffmpeg installed and in PATH?`);
			(spawnError as any).isSpawnError = true; // Flag spawn error
			reject(spawnError);
		});

		try { // Write input buffer to ffmpeg stdin
			ffmpegProcess.stdin.on('error', (err) => { logger.warn(`${logPrefix} Error writing to ffmpeg stdin (process might have exited): ${err.message}`); });
			ffmpegProcess.stdin.write(inputBuffer, (err) => {
				if (err && !ffmpegProcess.killed) logger.warn(`${logPrefix} Error writing buffer chunk to ffmpeg stdin: ${err.message}.`);
				if (!ffmpegProcess.stdin.destroyed) {
					ffmpegProcess.stdin.end((endErr: Error | null | undefined) => { // Signal end of input
						if (endErr && !ffmpegProcess.killed) logger.warn(`${logPrefix} Error ending ffmpeg stdin: ${endErr.message}.`);
					});
				}
			});
		} catch (error: any) { // Catch sync errors writing (e.g., stdin already closed)
			logger.error(`${logPrefix} Exception while writing to ffmpeg stdin: ${error.message}`);
			const stdinError = new Error(`Failed writing input to ffmpeg: ${error.message}`);
			(stdinError as any).isStdinError = true; // Flag stdin error
			reject(stdinError);
			if (!ffmpegProcess.killed) ffmpegProcess.kill(); // Ensure cleanup
		}
	});
}


// --- N8N Node Class Definition ---

export class WyomingTranscribeAudio implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Wyoming Transcribe Audio',
		name: 'wyomingTranscribeAudio',
		icon: 'file:transcribe.svg',
		group: ['transform'],
		version: 2,
		description: `Transcribes audio using the Wyoming protocol. Converts various input audio formats (e.g., MP3, WAV, Ogg, Opus) to the required ${WYOMING_AUDIO_FORMAT_DESC}. Requires 'ffmpeg' installed in the N8N environment's PATH.`,
		defaults: { name: 'Wyoming Transcribe' },
		inputs: ['main'],
		outputs: ['main'],
		properties: [
			{ displayName: 'Wyoming Server Address', name: 'wyomingServer', type: 'string', default: '127.0.0.1:10300', placeholder: 'e.g., 192.168.1.100:12101', description: 'Address (host:port) of your Wyoming Server (like whisper or piper)', required: true },
			{ displayName: 'Input Binary Field', name: 'inputBinaryField', type: 'string', default: 'data', placeholder: 'e.g., data', description: 'The name of the field containing the binary audio data. Supported input formats depend on the installed \'ffmpeg\' (e.g., MP3, WAV, Ogg, FLAC, AAC, Opus).', required: true },
			{ displayName: 'Output Field Name', name: 'outputFieldName', type: 'string', default: 'transcription', description: 'Field to store the transcribed text in', required: true },
			{ displayName: 'Language', name: 'language', type: 'string', default: 'en', placeholder: 'e.g., en, de, fr', description: 'Language code for transcription (ISO 639-1). Check server for supported languages.', required: true },
			{ displayName: 'Timeout (Ms)', name: 'timeoutMs', type: 'number', typeOptions: { minValue: 1000 }, default: 60000, description: 'Maximum time (milliseconds) to wait for the entire conversion and transcription process', required: true },
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const logger = this.logger;
		const possiblyRawPcmExtensions = new Set(['.pcm', '.raw', '.s16le', '.l16']);
		const possiblyRawPcmMimeTypes = new Set(['audio/l16', 'audio/basic', 'application/octet-stream']);
		logger.debug(`${NODE_NAME_LOG_PREFIX} Starting execution for ${items.length} items.`);

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			const item = items[itemIndex];
			const logPrefix = `${NODE_NAME_LOG_PREFIX} Item ${itemIndex}:`;
			const overallStartTime = performance.now();

			try {
				// --- Parameters & Input ---
				const serverAddress = this.getNodeParameter('wyomingServer', itemIndex) as string;
				const inputBinaryField = this.getNodeParameter('inputBinaryField', itemIndex) as string;
				const outputFieldName = this.getNodeParameter('outputFieldName', itemIndex) as string;
				const language = this.getNodeParameter('language', itemIndex, 'en') as string;
				const timeoutMs = this.getNodeParameter('timeoutMs', itemIndex, 60000) as number;
				const binaryData = item.binary?.[inputBinaryField];
				if (!binaryData) throw new NodeOperationError(this.getNode(), `No binary data found in input field "${inputBinaryField}".`, { itemIndex });
				const inputAudioBuffer = await this.helpers.getBinaryDataBuffer(itemIndex, inputBinaryField);
				if (!inputAudioBuffer || inputAudioBuffer.length === 0) throw new NodeOperationError(this.getNode(), `Binary data in input field "${inputBinaryField}" is present but empty.`, { itemIndex });
				const mimeType = binaryData.mimeType?.toLowerCase();
				const fileName = binaryData.fileName;
				const fileExtension = fileName ? path.extname(fileName).toLowerCase() : '';
				logger.debug(`${logPrefix} Input data Info: Filename='${fileName || 'N/A'}', MIME type='${mimeType || 'N/A'}', Extension='${fileExtension || 'N/A'}'`);

				// --- FFmpeg Conversion ---
				let pcmBufferToSend: Buffer;
				let conversionDurationMs = 0;
				if (mimeType === 'audio/wav' || (mimeType && possiblyRawPcmMimeTypes.has(mimeType)) || (fileExtension && possiblyRawPcmExtensions.has(fileExtension))) {
					logger.warn(`${logPrefix} Input format (${mimeType || fileExtension || 'unknown'}) might be WAV or raw PCM. Attempting conversion via ffmpeg anyway to ensure ${WYOMING_AUDIO_FORMAT_DESC}.`);
				}
				try {
					const conversionStartTime = performance.now();
					pcmBufferToSend = await convertAudioToPcm(inputAudioBuffer, fileName, SAMPLE_RATE, SAMPLE_CHANNELS, logger, logPrefix);
					const conversionEndTime = performance.now();
					conversionDurationMs = conversionEndTime - conversionStartTime;
					logger.info(`${logPrefix} FFmpeg conversion took ${conversionDurationMs.toFixed(2)} ms.`);
				} catch (conversionError: any) {
					const errorMessage = `FFmpeg Error: ${conversionError.message || String(conversionError)}`;
					// Propagate error flags if they exist on the original conversion error
					const errorOptions: Record<string, any> = { itemIndex, description: errorMessage };
					if ((conversionError as any).isFFmpegError) errorOptions.isFFmpegError = true;
					if ((conversionError as any).isSpawnError) errorOptions.isSpawnError = true; // Capture spawn errors here too
					throw new NodeOperationError(this.getNode(), errorMessage, errorOptions);
				}
				if (!pcmBufferToSend || pcmBufferToSend.length === 0) throw new NodeOperationError(this.getNode(), 'Audio conversion resulted in an empty buffer.', { itemIndex });

				// --- Wyoming Transcription ---
				let transcriptionDurationMs = 0;
				logger.debug(`${logPrefix} Starting transcription process.`);
				const transcriptionStartTime = performance.now();
				const transcription = await _transcribeAudio(this, itemIndex, serverAddress, pcmBufferToSend, language, timeoutMs);
				const transcriptionEndTime = performance.now();
				transcriptionDurationMs = transcriptionEndTime - transcriptionStartTime;
				logger.info(`${logPrefix} Wyoming transcription took ${transcriptionDurationMs.toFixed(2)} ms.`);

				// --- Prepare Output ---
				const newItemJson = JSON.parse(JSON.stringify(item.json));
				newItemJson[outputFieldName] = transcription;
				const newItem: INodeExecutionData = { json: newItemJson, pairedItem: { item: itemIndex } };
				returnData.push(newItem);
				const overallEndTime = performance.now();
				logger.debug(`${logPrefix} Successfully processed item in ${(overallEndTime - overallStartTime).toFixed(2)} ms (Conversion: ${conversionDurationMs.toFixed(2)} ms, Transcription: ${transcriptionDurationMs.toFixed(2)} ms).`);

			} catch (error: any) {
				// --- Refined Error Handling ---
				const errorMessage = error.message || String(error);
				const originalCode = (error as any).originalCode; // Socket error code
				const isSpawnError = (error as any).isSpawnError; // FFmpeg spawn error
				logger.error(`${logPrefix} Error processing item: ${errorMessage}`, error);
				const overallEndTime = performance.now();
				logger.debug(`${logPrefix} Failed processing item after ${(overallEndTime - overallStartTime).toFixed(2)} ms.`);

				// Critical errors: Network connection refused/unreachable/not found, or FFmpeg binary cannot be executed
				const isCriticalNetworkError = ['ECONNREFUSED', 'ENOTFOUND', 'EHOSTUNREACH', 'EAI_AGAIN'].includes(originalCode);
				const isCriticalError = isCriticalNetworkError || isSpawnError;

				if (isCriticalError) {
					// Always stop for critical errors, regardless of "Continue on Fail"
					logger.error(`${logPrefix} Critical error detected (${originalCode || 'ffmpeg spawn failed'}). Stopping workflow.`);
					if (error instanceof NodeOperationError) throw error;
					throw new NodeOperationError(this.getNode(), error as Error, { itemIndex, description: errorMessage });
				} else {
					// For non-critical errors (timeouts, server errors, ffmpeg *conversion* issues, etc.), respect "Continue on Fail"
					if (this.continueOnFail()) {
						logger.warn(`${logPrefix} Non-critical error occurred. continueOnFail is true. Recording error and continuing workflow.`);
						const n8nError = error instanceof NodeOperationError ? error : new NodeOperationError(this.getNode(), error as Error, { itemIndex, description: errorMessage });
						const errorItem: INodeExecutionData = { json: item.json, binary: item.binary, error: n8nError, pairedItem: { item: itemIndex } };
						returnData.push(errorItem);
					} else {
						logger.warn(`${logPrefix} Non-critical error occurred. continueOnFail is false. Stopping workflow.`);
						if (error instanceof NodeOperationError) throw error;
						throw new NodeOperationError(this.getNode(), error as Error, { itemIndex, description: errorMessage });
					}
				}
				// --- End of Refined Error Handling ---
			}
		} // End of loop over items

		logger.debug(`${NODE_NAME_LOG_PREFIX} Finished execution.`);
		return [returnData];
	}
}
