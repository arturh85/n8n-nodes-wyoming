import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	// Added for binary data handling
	IBinaryData
} from 'n8n-workflow';
import  {
	NodeOperationError,
} from 'n8n-workflow';
// Removed duplicate NodeOperationError import
import * as net from 'net';
import * as path from 'path';
import { spawn } from 'child_process'; // Import spawn for ffmpeg
import { performance } from 'perf_hooks'; // Import for high-resolution timing
import { WyomingCredentials } from '../../credentials/WyomingApi.credentials';

// --- Constants ---
const __version__ = '1.0.0'; // Wyoming protocol version compatibility
const SAMPLE_RATE = 16000; // Target sample rate for Wyoming (STT input, TTS output likely)
const SAMPLE_WIDTH = 2; // Target sample width (bytes) for Wyoming (16-bit) (STT input, TTS output likely)
const SAMPLE_CHANNELS = 1; // Target channels for Wyoming (Mono) (STT input, TTS output likely)
const NODE_NAME_LOG_PREFIX = '[WyomingNode]';
const NEWLINE = Buffer.from('\n', 'utf-8');
const WYOMING_AUDIO_FORMAT_DESC = `PCM, ${SAMPLE_RATE / 1000}kHz sample rate, ${SAMPLE_WIDTH * 8}-bit depth, Mono channel`;
const SYNTHESIZE_EVENT_TYPE = 'synthesize';
const TRANSCRIBE_EVENT_TYPE = 'transcribe';
const AUDIO_START_EVENT_TYPE = 'audio-start';
const AUDIO_CHUNK_EVENT_TYPE = 'audio-chunk';
const AUDIO_STOP_EVENT_TYPE = 'audio-stop';
const TRANSCRIPT_EVENT_TYPE = 'transcript';
const ERROR_EVENT_TYPE = 'error';
const DEFAULT_TTS_OUTPUT_MIME_TYPE = 'audio/wav'; // Output TTS as WAV for better compatibility

// Utility to yield the event loop, useful for async operations in tight loops or socket handling
const yieldEventLoop = () => new Promise(resolve => setImmediate(resolve));

// --- Standalone Helper Functions ---

/**
 * Creates a WAV header for raw PCM data.
 */
function _addWavHeader(
	pcmData: Buffer,
	sampleRate: number,
	sampleWidthBytes: number,
	channels: number
): Buffer {
	const bitsPerSample = sampleWidthBytes * 8;
	const blockAlign = channels * sampleWidthBytes;
	const byteRate = sampleRate * blockAlign;
	const dataSize = pcmData.length;
	const fileSize = 36 + dataSize; // 44 bytes total header size minus 8 bytes for RIFF id and size

	const header = Buffer.alloc(44);

	// RIFF chunk descriptor
	header.write('RIFF', 0);
	header.writeUInt32LE(fileSize, 4); // file-size (total size - 8 bytes)
	header.write('WAVE', 8);

	// fmt sub-chunk
	header.write('fmt ', 12);
	header.writeUInt32LE(16, 16); // Subchunk1Size (16 for PCM)
	header.writeUInt16LE(1, 20);  // AudioFormat (1 for PCM)
	header.writeUInt16LE(channels, 22);
	header.writeUInt32LE(sampleRate, 24);
	header.writeUInt32LE(byteRate, 28);
	header.writeUInt16LE(blockAlign, 32);
	header.writeUInt16LE(bitsPerSample, 34);

	// data sub-chunk
	header.write('data', 36);
	header.writeUInt32LE(dataSize, 40);

	return Buffer.concat([header, pcmData]);
}


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
		// logger.trace(`${logPrefix} Sending Primary JSON: ${primaryJsonString}`); // Optional deeper tracing
		// if (dataBytes) logger.trace(`${logPrefix} Sending Data JSON: ${dataBytes.toString('utf-8')}`); // Optional
		// if (payload) logger.trace(`${logPrefix} Sending Payload: ${payload.length} bytes`); // Optional

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
		await yieldEventLoop(); // Allow potential error handling on socket
		return false; // Indicate failure
	}
	return overallSuccess;
}

/**
 * Common logic for handling Wyoming connection and event parsing.
 */
async function _handleWyomingCommunication<T>(
	execContext: IExecuteFunctions,
	itemIndex: number,
	serverAddress: string,
	timeoutMs: number,
	initialSendLogic: (client: net.Socket, logPrefix: string) => Promise<void>,
	processReceivedEvent: (
		eventType: string,
		eventData: Record<string, any> | null,
		payload: Buffer | null,
		state: { audioChunks: Buffer[], done: boolean, result?: T, error?: Error }, // Mutable state object
		logPrefix: string
	) => Promise<void>,
	getFinalResult: (state: { audioChunks: Buffer[], done: boolean, result?: T, error?: Error }) => T | Promise<T>,
	operationType: 'STT' | 'TTS' // For logging
): Promise<T> {
	const logger = execContext.logger;
	const logPrefix = `${NODE_NAME_LOG_PREFIX} Item ${itemIndex} (${operationType}):`;

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
		let connectionClosed = false;
		let appTimeoutHandle: NodeJS.Timeout | null = null;
		// Mutable state to be passed to event processor
		const processingState: { audioChunks: Buffer[], done: boolean, result?: T, error?: Error } = {
			audioChunks: [],
			done: false,
		};

		const cleanup = async (reason?: string) => {
			if (appTimeoutHandle) { clearTimeout(appTimeoutHandle); appTimeoutHandle = null; }
			if (!connectionClosed) {
				connectionClosed = true;
				if (!client.destroyed) {
					logger.debug(`${logPrefix} Cleaning up connection (${reason || 'normal close'}). State: ${client.readyState}`);
					await yieldEventLoop();
					client.removeAllListeners();
					client.end(); // Graceful shutdown
					client.destroySoon(); // Ensure it gets destroyed
				}
			}
		};

		appTimeoutHandle = setTimeout(async () => {
			const errorMsg = `Application timeout reached after ${timeoutMs}ms waiting for ${operationType} result`;
			logger.warn(`${logPrefix} ${errorMsg}`);
			await cleanup('application timeout');
			if (!processingState.done && !connectionClosed) {
				const timeoutError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				(timeoutError as any).isTimeout = true; // Flag timeout error
				reject(timeoutError);
			}
		}, timeoutMs);

		client.on('connect', async () => {
			logger.info(`${logPrefix} TCP connection established with ${host}:${port}.`);
			await yieldEventLoop();
			try {
				await initialSendLogic(client, logPrefix);
				logger.debug(`${logPrefix} Successfully sent initial request sequence.`);
				await yieldEventLoop();
			} catch (err: any) {
				const errorMsg = `Error during initial data sending sequence: ${err.message}`;
				logger.error(`${logPrefix} ${errorMsg}`, err);
				await cleanup('write error sequence');
				// Reject only if the timeout hasn't already fired or connection isn't closed
				if (appTimeoutHandle && !connectionClosed) {
				 reject(new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex })); // Don't flag specific type here
				}
			}
		});

		client.on('data', async (chunk: Buffer) => {
			receivedDataBuffer = Buffer.concat([receivedDataBuffer, chunk]);
			// logger.trace(`${logPrefix} Received chunk: ${chunk.length} bytes. Total buffer: ${receivedDataBuffer.length} bytes.`);
			await yieldEventLoop();

			// Process all complete events in the buffer
			while (true) {
				// 1. Find the end of the primary JSON line
				const newlineIndex = receivedDataBuffer.indexOf(NEWLINE);
				if (newlineIndex === -1) {
					// Not enough data for even the primary JSON line yet. Wait for more.
					// logger.trace(`${logPrefix} Incomplete primary JSON line. Waiting for more data.`);
					break;
				}

				// 2. Extract and parse the primary JSON
				const primaryJsonBuffer = receivedDataBuffer.subarray(0, newlineIndex);
				let eventDict: Record<string, any>;
				try {
					const primaryJsonString = primaryJsonBuffer.toString('utf-8');
					// logger.trace(`${logPrefix} Attempting to parse primary JSON: ${primaryJsonString}`);
					eventDict = JSON.parse(primaryJsonString);
				} catch (parseError: any) {
					logger.error(`${logPrefix} Failed to parse Primary JSON Line: "${primaryJsonBuffer.toString('utf-8')}". Error: ${parseError.message}. Discarding line.`);
					// Consume the invalid line including the newline
					receivedDataBuffer = receivedDataBuffer.subarray(newlineIndex + NEWLINE.length);
					await yieldEventLoop();
					continue; // Try processing the rest of the buffer
				}

				// 3. Get data and payload lengths from the parsed primary JSON
				const dataLength = eventDict['data_length'] as number | undefined ?? 0;
				const payloadLength = eventDict['payload_length'] as number | undefined ?? 0;

				// 4. Calculate the total length of the *entire* message (including JSON line, newline, data, payload)
				const messageJsonLineLength = primaryJsonBuffer.length + NEWLINE.length;
				const messageDataPayloadLength = dataLength + payloadLength;
				const fullMessageLength = messageJsonLineLength + messageDataPayloadLength;

				// 5. Check if the *entire* message is present in the buffer
				if (receivedDataBuffer.length < fullMessageLength) {
					// The full message (JSON + data + payload) hasn't arrived yet. Wait for more data.
					// logger.trace(`${logPrefix} Incomplete event data/payload. Need ${fullMessageLength}, have ${receivedDataBuffer.length}. Waiting.`);
					break;
				}

				// --- We now have the complete message in the buffer ---

				// 6. Extract Data block (if present)
				let dataDict: object | null = eventDict['data'] || null; // Data might be inline
				let dataBytes: Buffer | null = null;
				if (dataLength > 0) {
					const dataStart = messageJsonLineLength;
					const dataEnd = dataStart + dataLength;
					dataBytes = receivedDataBuffer.subarray(dataStart, dataEnd);
					try {
						const separateDataDict = JSON.parse(dataBytes.toString('utf-8'));
						if (!dataDict) dataDict = {};
						Object.assign(dataDict, separateDataDict); // Merge inline and separate data
					} catch (parseError: any) {
						logger.error(`${logPrefix} Failed to parse separate Data Bytes: ${dataBytes.toString('utf-8')}. Error: ${parseError.message}. Ignoring data part for this event.`);
						dataDict = dataDict || {}; // Keep potential inline data
					}
				}

				// 7. Extract Payload block (if present)
				let payloadBytes: Buffer | null = null;
				if (payloadLength > 0) {
					const payloadStart = messageJsonLineLength + dataLength;
					const payloadEnd = payloadStart + payloadLength;
					payloadBytes = receivedDataBuffer.subarray(payloadStart, payloadEnd);
				}

				// 8. Consume the *entire* processed message from the buffer
				receivedDataBuffer = receivedDataBuffer.subarray(fullMessageLength);
				await yieldEventLoop(); // Allow event loop turn after buffer modification

				// 9. Process the received event
				const eventType = eventDict['type'] as string;
				// logger.info(`${logPrefix} Rcvd & Parsed Complete Event: Type=${eventType}, Data=${JSON.stringify(dataDict)}, PayloadLength=${payloadBytes?.length ?? 0}`);
				await yieldEventLoop();

				try {
					// Pass the extracted data and payload to the specific handler
					await processReceivedEvent(eventType, dataDict, payloadBytes, processingState, logPrefix);
				} catch (processorError: any) {
					logger.error(`${logPrefix} Error processing received event type ${eventType}: ${processorError.message}`, processorError);
					processingState.error = processorError instanceof NodeOperationError ? processorError : new NodeOperationError(execContext.getNode(), processorError.message || String(processorError), { itemIndex });
					processingState.done = true; // Mark as done due to error
				}

				// 10. Check if the operation is complete (success or error)
				if (processingState.done) {
					logger.debug(`${logPrefix} Operation marked as done. Cleaning up.`);
					await cleanup(processingState.error ? 'error processing event' : 'operation complete');
					if (processingState.error) {
						reject(processingState.error);
					} else {
						try {
							const finalResult = await getFinalResult(processingState);
							resolve(finalResult);
						} catch (resultError: any)
						 {
							logger.error(`${logPrefix} Error finalizing result: ${resultError.message}`, resultError);
							reject(resultError instanceof NodeOperationError ? resultError : new NodeOperationError(execContext.getNode(), resultError.message || String(resultError), { itemIndex }));
						}
					}
					return; // Exit the 'data' handler completely
				}

				// If buffer is empty after processing, break the inner loop, otherwise continue processing
				if (receivedDataBuffer.length === 0) {
					// logger.trace(`${logPrefix} Buffer empty after processing event. Waiting for more data.`);
					break;
				} else {
					// logger.trace(`${logPrefix} ${receivedDataBuffer.length} bytes remaining in buffer. Checking for next event.`);
				}

			} // End while(true) loop for processing buffer
		}); // End client.on('data')

		client.on('end', async () => {
			await cleanup('server ended connection');
			// Reject only if the operation wasn't already completed/timed out/closed
			if (!processingState.done && appTimeoutHandle && !connectionClosed) {
				const errorMsg = `Connection closed by server before ${operationType} completed.`;
				logger.warn(`${logPrefix} ${errorMsg}`);
				const closeError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				(closeError as any).isConnectionClosed = true; // Flag premature close
				reject(closeError);
			}
		});

		client.on('close', async (hadError: boolean) => {
			const cleanupReason = `closed${hadError ? ' with error' : ''}`;
			const shouldReject = !processingState.done && appTimeoutHandle && !connectionClosed; // Check if rejection is still relevant
			await cleanup(cleanupReason); // Ensure cleanup happens
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
			const shouldReject = !processingState.done && appTimeoutHandle && !connectionClosed; // Check if rejection is still relevant
			await cleanup('socket error'); // Ensure cleanup happens
			if (shouldReject) {
				const nodeError = new NodeOperationError(execContext.getNode(), errorMsg, { itemIndex });
				(nodeError as any).originalCode = code; // Preserve original code
				if (code === 'ECONNREFUSED' || code === 'ENOTFOUND' || code === 'EHOSTUNREACH') {
					(nodeError as any).isConfigurationError = true; // Likely bad address/port or server down
				}
				reject(nodeError);
			}
		});

		// Optional: Handle socket timeout if the server library uses it
		// client.on('timeout', async () => {
		//  logger.warn(`${logPrefix} Socket inactivity timeout triggered.`);
		//  await cleanup('socket inactivity timeout');
		//  // Optional: reject if needed and not already done
		// });

		// Drain event indicates the write buffer is empty again
		client.on('drain', async () => {
			// logger.trace(`${logPrefix} Socket write buffer drained.`);
			await yieldEventLoop();
		});

		logger.debug(`${logPrefix} Attempting to connect to ${host}:${port}...`);
		await yieldEventLoop();
		client.connect(port, host);
	});
}

/**
 * Handles the audio transcription process (STT).
 */
async function _transcribeAudio(
	execContext: IExecuteFunctions,
	itemIndex: number,
	serverAddress: string,
	audioBuffer: Buffer, // Already converted PCM buffer
	language: string,
	timeoutMs: number,
): Promise<string> {

	const initialSendLogic = async (client: net.Socket, logPrefix: string) => {
		let overallWriteSuccess = true;
		overallWriteSuccess &&= await _sendWyomingEventRevised(client, TRANSCRIBE_EVENT_TYPE, { language }, null, execContext.logger, logPrefix);
		overallWriteSuccess &&= await _sendWyomingEventRevised(client, AUDIO_START_EVENT_TYPE, { rate: SAMPLE_RATE, width: SAMPLE_WIDTH, channels: SAMPLE_CHANNELS }, null, execContext.logger, logPrefix);
		// Send audio in chunks if it's very large? For now, send all at once.
		overallWriteSuccess &&= await _sendWyomingEventRevised(client, AUDIO_CHUNK_EVENT_TYPE, { rate: SAMPLE_RATE, width: SAMPLE_WIDTH, channels: SAMPLE_CHANNELS }, audioBuffer, execContext.logger, logPrefix);
		overallWriteSuccess &&= await _sendWyomingEventRevised(client, AUDIO_STOP_EVENT_TYPE, {}, null, execContext.logger, logPrefix);
		if (!overallWriteSuccess) execContext.logger.warn(`${logPrefix} Completed sending STT sequence, but kernel buffer reported full at some point.`);
	};

	const processReceivedEvent = async (
		eventType: string,
		eventData: Record<string, any> | null,
		_payload: Buffer | null, // Payload not expected for transcript/error
		state: { audioChunks: Buffer[], done: boolean, result?: string, error?: Error },
		logPrefix: string
	) => {
		if (eventType === TRANSCRIPT_EVENT_TYPE && eventData && typeof eventData.text === 'string') {
			const transcription = eventData.text;
			execContext.logger.info(`${logPrefix} Transcription successful: "${transcription}"`);
			state.result = transcription;
			state.done = true;
		} else if (eventType === ERROR_EVENT_TYPE) {
			const errMsg = eventData?.message || 'Unknown server error during transcription';
			execContext.logger.error(`${logPrefix} Received error event from server: ${errMsg}`);
			const serverError = new NodeOperationError(execContext.getNode(), `Wyoming server error (STT): ${errMsg}`, { itemIndex });
			(serverError as any).isServerError = true; // Flag server-reported error
			state.error = serverError;
			state.done = true;
		} else {
			execContext.logger.warn(`${logPrefix} Received unhandled/ignored event type during STT: ${eventType}`);
		}
	};

	const getFinalResult = (state: { audioChunks: Buffer[], done: boolean, result?: string, error?: Error }): string => {
		if (state.result !== undefined) {
			return state.result;
		}
		// This should ideally not be reached if 'done' is true without a result or error
		throw new NodeOperationError(execContext.getNode(), 'Transcription finished unexpectedly without result or error.', { itemIndex });
	};

	return _handleWyomingCommunication<string>(
		execContext,
		itemIndex,
		serverAddress,
		timeoutMs,
		initialSendLogic,
		processReceivedEvent,
		getFinalResult,
		'STT'
	);
}

/**
 * Handles the audio synthesis process (TTS).
 */
async function _synthesizeAudio(
	execContext: IExecuteFunctions,
	itemIndex: number,
	serverAddress: string,
	textToSpeak: string,
	voiceName: string | undefined, // Allow undefined
	timeoutMs: number,
): Promise<Buffer> { // Returns raw PCM buffer

	const initialSendLogic = async (client: net.Socket, logPrefix: string) => {
		const synthesizeData: Record<string, any> = { text: textToSpeak };
		// Construct voice object based on provided parameters
		// Prioritize voice name if given
		if (voiceName) {
			synthesizeData.voice = { name: voiceName };
			// Note: Wyoming spec allows speaker within name, but keeping it simple here.
			// If Piper needs speaker ID separate, adjust this structure:
			// synthesizeData.voice = { name: 'some-voice-model', speaker: voiceName };
		}
		// If neither voice nor language is provided, don't include the voice field
        // and let the server use its default.

		const success = await _sendWyomingEventRevised(client, SYNTHESIZE_EVENT_TYPE, synthesizeData, null, execContext.logger, logPrefix);
		if (!success) {
			// Throw an error to be caught in the connect handler of _handleWyomingCommunication
			throw new NodeOperationError(execContext.getNode(), "Failed to send synthesize event (write buffer likely full immediately).");
		}
	};

	const processReceivedEvent = async (
		eventType: string,
		eventData: Record<string, any> | null,
		payload: Buffer | null,
		state: { audioChunks: Buffer[], done: boolean, result?: Buffer, error?: Error }, // result is not used here, audioChunks holds the data
		logPrefix: string
	) => {
		if (eventType === AUDIO_START_EVENT_TYPE) {
			// Optional: Log or verify the received audio format
			const rate = eventData?.rate ?? 'unknown';
			const width = eventData?.width ?? 'unknown';
			const channels = eventData?.channels ?? 'unknown';
			execContext.logger.info(`${logPrefix} Received audio-start. Format: ${rate} Hz, ${width} bytes/sample, ${channels} channel(s).`);
			if (rate !== SAMPLE_RATE || width !== SAMPLE_WIDTH || channels !== SAMPLE_CHANNELS) {
				execContext.logger.warn(`${logPrefix} Received audio format differs from node defaults. Using received format for WAV header.`);
				// Store format if needed later, but WAV header currently uses constants
			}
			state.audioChunks = []; // Reset chunks on new start
		} else if (eventType === AUDIO_CHUNK_EVENT_TYPE) {
			if (payload) {
				// execContext.logger.trace(`${logPrefix} Received audio chunk: ${payload.length} bytes.`);
				state.audioChunks.push(payload);
			} else {
				execContext.logger.warn(`${logPrefix} Received audio-chunk event with no payload.`);
			}
		} else if (eventType === AUDIO_STOP_EVENT_TYPE) {
			execContext.logger.info(`${logPrefix} Received audio-stop. Synthesis complete.`);
			state.done = true; // Mark as done, result will be assembled in getFinalResult
		} else if (eventType === ERROR_EVENT_TYPE) {
			const errMsg = eventData?.message || 'Unknown server error during synthesis';
			execContext.logger.error(`${logPrefix} Received error event from server: ${errMsg}`);
			const serverError = new NodeOperationError(execContext.getNode(), `Wyoming server error (TTS): ${errMsg}`, { itemIndex });
			(serverError as any).isServerError = true;
			state.error = serverError;
			state.done = true;
		} else {
			execContext.logger.warn(`${logPrefix} Received unhandled/ignored event type during TTS: ${eventType}`);
		}
	};

	const getFinalResult = (state: { audioChunks: Buffer[], done: boolean, result?: Buffer, error?: Error }): Buffer => {
		if (state.audioChunks.length > 0) {
			return Buffer.concat(state.audioChunks);
		} else if (state.error) {
            // Should have been rejected already, but handle defensively
            throw state.error;
        } else {
			// We got audio-stop but no chunks, or finished some other way without error/chunks
			execContext.logger.warn(`[WyomingNode] Synthesis finished but no audio data was received.`);
			return Buffer.alloc(0); // Return empty buffer
		}
	};

	return _handleWyomingCommunication<Buffer>(
		execContext,
		itemIndex,
		serverAddress,
		timeoutMs,
		initialSendLogic,
		processReceivedEvent,
		getFinalResult,
		'TTS'
	);
}


/**
 * Converts audio buffer to the target PCM format (s16le, 16kHz, mono) using ffmpeg.
 * Adds flags to rejected errors for specific failure types. (Used only for STT)
 */
async function convertAudioToPcm(
	inputBuffer: Buffer,
	_inputFileNameHint: string | undefined, // Keep signature, but might not use hint
	targetSampleRate: number,
	targetChannels: number,
	logger: IExecuteFunctions['logger'],
	logPrefix: string,
): Promise<Buffer> {

	const ffmpegArgs = [
		'-loglevel', 'error', // Only output errors
		'-i', 'pipe:0',        // Input from stdin
		'-f', 's16le',         // Output format: signed 16-bit little-endian PCM
		'-ar', String(targetSampleRate), // Output sample rate
		'-ac', String(targetChannels),  // Output channels (mono)
		'-',                   // Output to stdout
	];
	logger.info(`${logPrefix} Starting audio conversion with ffmpeg. Target: ${targetSampleRate}Hz, ${targetChannels === 1 ? 'Mono' : 'Stereo'}.`);
	logger.debug(`${logPrefix} Running ffmpeg command: ffmpeg ${ffmpegArgs.join(' ')}`);

	return new Promise<Buffer>((resolve, reject) => {
		const ffmpegProcess = spawn('ffmpeg', ffmpegArgs, { stdio: ['pipe', 'pipe', 'pipe'] });
		const outputChunks: Buffer[] = [];
		const errorChunks: Buffer[] = [];

		// Handle stdout
		ffmpegProcess.stdout.on('data', (chunk: Buffer) => {
			outputChunks.push(chunk);
		});

		// Handle stderr
		ffmpegProcess.stderr.on('data', (chunk: Buffer) => {
			errorChunks.push(chunk);
		});

		// Handle process exit
		ffmpegProcess.on('close', (code) => {
			const stderrOutput = Buffer.concat(errorChunks).toString('utf-8').trim();
			if (code !== 0) {
				logger.error(`${logPrefix} ffmpeg process exited with code ${code}.`);
				if (stderrOutput) logger.error(`${logPrefix} ffmpeg stderr: ${stderrOutput}`);
				else logger.error(`${logPrefix} ffmpeg stderr: (empty)`);

				// Try to provide a more specific error message
				let userMessage = `ffmpeg conversion failed (exit code ${code})`;
				if (stderrOutput.includes('Invalid data found when processing input')) {
					userMessage += ': Input audio data seems invalid or corrupted.';
				} else if (stderrOutput.includes('Output file #0 does not contain any stream')) {
					userMessage += ': ffmpeg could not produce output, possibly due to input format issues or invalid parameters.';
				} else if (stderrOutput) {
					userMessage += `: ${stderrOutput}`;
				}

				const ffmpegError = new Error(userMessage);
				(ffmpegError as any).isFFmpegError = true; // Flag ffmpeg execution error
				reject(ffmpegError);
			} else {
				const pcmBuffer = Buffer.concat(outputChunks);
				if (pcmBuffer.length === 0) {
					logger.error(`${logPrefix} ffmpeg conversion succeeded (code 0) but produced empty output.`);
					if (stderrOutput) logger.warn(`${logPrefix} ffmpeg stderr (may contain clues): ${stderrOutput}`);
					const ffmpegError = new Error('ffmpeg conversion produced empty output.');
					(ffmpegError as any).isFFmpegError = true; // Flag empty output as ffmpeg related
					reject(ffmpegError);
				} else if (pcmBuffer.length % SAMPLE_WIDTH !== 0) {
                    logger.warn(`${logPrefix} Converted PCM buffer length (${pcmBuffer.length} bytes) is not a multiple of sample width (${SAMPLE_WIDTH}). This might indicate an issue.`);
                    if (stderrOutput) logger.warn(`${logPrefix} ffmpeg stderr: ${stderrOutput}`);
					logger.info(`${logPrefix} ffmpeg conversion successful despite odd length. Resulting PCM buffer size: ${pcmBuffer.length} bytes.`);
					if (stderrOutput) logger.warn(`${logPrefix} ffmpeg stderr (possibly warnings): ${stderrOutput}`);
					resolve(pcmBuffer);
				} else {
					logger.info(`${logPrefix} ffmpeg conversion successful. Resulting PCM buffer size: ${pcmBuffer.length} bytes.`);
					if (stderrOutput) logger.warn(`${logPrefix} ffmpeg stderr (possibly warnings): ${stderrOutput}`);
					resolve(pcmBuffer);
				}
			}
		});

		// Handle spawn errors (e.g., ffmpeg not found)
		ffmpegProcess.on('error', (err) => {
			logger.error(`${logPrefix} Failed to start ffmpeg process: ${err.message}`);
			const spawnError = new Error(`Failed to spawn ffmpeg: ${err.message}. Is ffmpeg installed and in PATH?`);
			(spawnError as any).isSpawnError = true; // Flag spawn error
			reject(spawnError);
		});

		// Handle errors writing to stdin (e.g., process already closed)
		ffmpegProcess.stdin.on('error', (err: NodeJS.ErrnoException) => {
			// Ignore EPIPE errors, which happen if ffmpeg closes stdin before we finish writing (e.g., due to an input error)
			if (err.code !== 'EPIPE') {
				logger.warn(`${logPrefix} Error writing to ffmpeg stdin (process might have exited): ${err.message} (${err.code})`);
			}
		});

		// Write the input buffer to ffmpeg's stdin
		try {
			ffmpegProcess.stdin.write(inputBuffer, (err) => {
				if (err && !ffmpegProcess.killed && (err as NodeJS.ErrnoException).code !== 'EPIPE') {
					logger.warn(`${logPrefix} Error writing buffer chunk to ffmpeg stdin: ${err.message}.`);
				}
				// Close stdin after writing is complete (or errored)
				if (!ffmpegProcess.stdin.destroyed) {
					ffmpegProcess.stdin.end((endErr: Error | null | undefined) => {
						if (endErr && !ffmpegProcess.killed && (endErr as NodeJS.ErrnoException).code !== 'EPIPE') {
							logger.warn(`${logPrefix} Error ending ffmpeg stdin: ${endErr.message}.`);
						}
					});
				}
			});
		} catch (error: any) {
			// Catch synchronous errors, though less likely here
			logger.error(`${logPrefix} Exception while initiating write to ffmpeg stdin: ${error.message}`);
			const stdinError = new Error(`Failed writing input to ffmpeg: ${error.message}`);
			(stdinError as any).isStdinError = true; // Flag stdin error
			reject(stdinError);
			if (!ffmpegProcess.killed) {
				ffmpegProcess.kill(); // Ensure cleanup
			}
		}
	});
}


// --- N8N Node Class Definition ---

export class WyomingNode implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Wyoming Protocol',
		name: 'wyomingNode',
		icon: 'file:transcribe.svg',
		group: ['transform'],
		version: 2,
		description: `Performs Speech-to-Text (STT) or Text-to-Speech (TTS) using the Wyoming protocol. Connects to servers like Piper (TTS) or Whisper (STT). Requires 'ffmpeg' for STT audio conversion. STT Input: Various formats (MP3, WAV, Ogg, etc.). STT Output: Text. TTS Input: Text. TTS Output: WAV Audio (${WYOMING_AUDIO_FORMAT_DESC} internally).`,
		defaults: { name: 'Wyoming Node' },
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'wyomingApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Speech to Text (STT)',
						value: 'stt',
						description: 'Convert audio input to text',
						action: 'Speech to text',
					},
					{
						name: 'Text to Speech (TTS)',
						value: 'tts',
						description: 'Convert text input to audio',
						action: 'Text to speech',
					},
				],
				default: 'tts',
				description: 'Choose whether to convert speech to text or text to speech',
			},

			// // Common Properties
			// {
			// 	displayName: 'Wyoming Server Address',
			// 	name: 'wyomingServer',
			// 	type: 'string',
			// 	default: '127.0.0.1:10300', // Default Whisper port? Adjust if needed
			// 	placeholder: 'e.g., 192.168.1.100:10300',
			// 	description: 'Address (host:port) of your Wyoming Server (e.g., Whisper for STT, Piper for TTS)',
			// 	required: true,
			// },
			{
				displayName: 'Language',
				name: 'language',
				type: 'string',
				default: 'en',
				placeholder: 'e.g., en, de, fr, en-us',
				displayOptions: {
					show: {
						operation: ['stt'],
					},
				},
				description: 'Language code (e.g., ISO 639-1 or IETF). Specifies transcription language. Check server documentation for supported codes/formats.',
			},
			{
				displayName: 'Timeout (Ms)',
				name: 'timeoutMs',
				type: 'number',
				typeOptions: { minValue: 1000 },
				default: 60000,
				description: 'Maximum time (milliseconds) to wait for the entire operation (connection, processing, response)',
				required: true,
			},

			// --- STT Specific Properties ---
			{
				displayName: 'Input Binary Field (Audio)',
				name: 'inputBinaryField',
				type: 'string',
				default: 'data',
				required: true,
				displayOptions: {
					show: {
						operation: ['stt'],
					},
				},
				placeholder: 'e.g., data',
				description: 'Name of the field containing the input binary audio data for STT. Supported formats depend on installed \'ffmpeg\' (e.g., MP3, WAV, Ogg, FLAC, AAC, Opus).',
			},
			{
				displayName: 'Output Field Name (Text)',
				name: 'outputFieldNameStt', // Changed name to avoid conflict
				type: 'string',
				default: 'transcription', // More specific default
				required: true,
				displayOptions: {
					show: {
						operation: ['stt'],
					},
				},
				description: 'Field name where the resulting transcribed text will be stored',
			},

			// --- TTS Specific Properties ---
			{
				displayName: 'Input Text',
				name: 'inputText',
				type: 'string',
				default: '',
				required: true,
				displayOptions: {
					show: {
						operation: ['tts'],
					},
				},
				placeholder: 'e.g., message',
				description: 'Text to be synthesized into speech',
			},
			{
				displayName: 'Voice Name/ID',
				name: 'voice',
				type: 'string',
				default: 'en_US-lessac-medium',
				required: true,
				displayOptions: {
					show: {
						operation: ['tts'],
					},
				},
				placeholder: 'e.g., en_US-lessac-medium or specific speaker ID',
				description: 'Name or ID of the voice to use for TTS. If omitted, the server\'s default or a voice matching the Language might be used. Check your TTS server (e.g., Piper) documentation for available voices.',
			},
			{
				displayName: 'Output Field Name (Audio)',
				name: 'outputFieldName',
				type: 'string',
				default: 'audio',
				required: true,
				displayOptions: {
					show: {
						operation: ['tts'],
					},
				},
				description: `Field name where the resulting synthesized audio (as binary WAV data) will be stored. MIME Type: ${DEFAULT_TTS_OUTPUT_MIME_TYPE}.`,
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const logger = this.logger;

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			const item = items[itemIndex];
			const logPrefix = `${NODE_NAME_LOG_PREFIX} Item ${itemIndex}:`;
			const overallStartTime = performance.now();

			// --- Common Parameters ---
			const operation = this.getNodeParameter('operation', itemIndex) as 'stt' | 'tts';
			const credentials = await this.getCredentials('wyomingApi') as WyomingCredentials;
			const serverAddress = `${credentials.host}:${credentials.port}`;
			const timeoutMs = this.getNodeParameter('timeoutMs', itemIndex, 60000) as number;

			logger.info(`${logPrefix} serverAddress: ${serverAddress}`);
			logger.info(`${logPrefix} Starting operation: ${operation.toUpperCase()}`);

			try {
				let newItem: INodeExecutionData | null = null; // Initialize as null

				// --- STT Execution Path ---
				if (operation === 'stt') {
					const language = this.getNodeParameter('language', itemIndex, undefined) as string | undefined; // Allow undefined
					const inputBinaryField = this.getNodeParameter('inputBinaryField', itemIndex) as string;
					const outputFieldName = this.getNodeParameter('outputFieldNameStt', itemIndex) as string; // Use STT output field

					const binaryData = item.binary?.[inputBinaryField];
					if (!binaryData) throw new NodeOperationError(this.getNode(), `STT: No binary data found in input field "${inputBinaryField}".`, { itemIndex });

					const inputAudioBuffer = await this.helpers.getBinaryDataBuffer(itemIndex, inputBinaryField);
					if (!inputAudioBuffer || inputAudioBuffer.length === 0) throw new NodeOperationError(this.getNode(), `STT: Binary data in input field "${inputBinaryField}" is present but empty.`, { itemIndex });

					const mimeType = binaryData.mimeType?.toLowerCase();
					const fileName = binaryData.fileName;
					const fileExtension = fileName ? path.extname(fileName).toLowerCase() : '';
					logger.debug(`${logPrefix} STT Input Info: Filename='${fileName || 'N/A'}', MIME type='${mimeType || 'N/A'}', Extension='${fileExtension || 'N/A'}'`);

					// --- FFmpeg Conversion (STT Only) ---
					let pcmBufferToSend: Buffer;
					let conversionDurationMs = 0;
					// Always convert for STT to ensure correct format
					logger.info(`${logPrefix} STT: Preparing audio using ffmpeg conversion to ${WYOMING_AUDIO_FORMAT_DESC}.`);
					try {
						const conversionStartTime = performance.now();
						pcmBufferToSend = await convertAudioToPcm(inputAudioBuffer, fileName, SAMPLE_RATE, SAMPLE_CHANNELS, logger, logPrefix);
						const conversionEndTime = performance.now();
						conversionDurationMs = conversionEndTime - conversionStartTime;
						logger.info(`${logPrefix} STT: FFmpeg conversion took ${conversionDurationMs.toFixed(2)} ms.`);
					} catch (conversionError: any) {
						const errorMessage = `STT Error during ffmpeg conversion: ${conversionError.message || String(conversionError)}`;
						const errorOptions: Record<string, any> = { itemIndex, description: errorMessage };
						if ((conversionError as any).isFFmpegError) errorOptions.isFFmpegError = true;
						if ((conversionError as any).isSpawnError) errorOptions.isSpawnError = true;
						throw new NodeOperationError(this.getNode(), errorMessage, errorOptions);
					}
					if (!pcmBufferToSend || pcmBufferToSend.length === 0) {
						throw new NodeOperationError(this.getNode(), 'STT: Audio conversion resulted in an empty buffer.', { itemIndex });
					}

					// --- Wyoming Transcription ---
					let transcriptionDurationMs = 0;
					logger.debug(`${logPrefix} STT: Starting transcription process.`);
					const transcriptionStartTime = performance.now();
					// Ensure language is provided for STT, default to 'en' if undefined
					const sttLanguage = language || 'en';
					logger.debug(`${logPrefix} STT: Using language code: ${sttLanguage}`);
					const transcription = await _transcribeAudio(this, itemIndex, serverAddress, pcmBufferToSend, sttLanguage, timeoutMs);
					const transcriptionEndTime = performance.now();
					transcriptionDurationMs = transcriptionEndTime - transcriptionStartTime;
					logger.info(`${logPrefix} STT: Wyoming transcription took ${transcriptionDurationMs.toFixed(2)} ms.`);

					// --- Prepare STT Output ---
					const newItemJson = JSON.parse(JSON.stringify(item.json)); // Deep copy
					newItemJson[outputFieldName] = transcription;
					newItem = { json: newItemJson, pairedItem: { item: itemIndex } };

					const overallEndTime = performance.now();
					logger.info(`${logPrefix} STT: Successfully processed item in ${(overallEndTime - overallStartTime).toFixed(2)} ms (Conversion: ${conversionDurationMs.toFixed(2)} ms, Transcription: ${transcriptionDurationMs.toFixed(2)} ms).`);

				}
				// --- TTS Execution Path ---
				else if (operation === 'tts') {
					const voice = this.getNodeParameter('voice', itemIndex, undefined) as string | undefined; // Optional voice
					const outputFieldName = this.getNodeParameter('outputFieldName', itemIndex) as string; // Use TTS output field
					const textToSpeak = this.getNodeParameter('inputText', itemIndex) as string;
					if (typeof textToSpeak !== 'string' || textToSpeak.trim().length === 0) {
						throw new NodeOperationError(this.getNode(), `TTS: Input text from parameter "${textToSpeak}" is missing or empty.`, { itemIndex });
					}
					if(voice) logger.debug(`${logPrefix} TTS: Using voice: ${voice}`);

					// --- Wyoming Synthesis ---
					let synthesisDurationMs = 0;
					logger.debug(`${logPrefix} TTS: Starting synthesis process for text: "${textToSpeak.substring(0, 50)}${textToSpeak.length > 50 ? '...' : ''}"`);
					const synthesisStartTime = performance.now();
					const rawPcmAudioBuffer = await _synthesizeAudio(this, itemIndex, serverAddress, textToSpeak, voice, timeoutMs);
					const synthesisEndTime = performance.now();
					synthesisDurationMs = synthesisEndTime - synthesisStartTime;
					logger.info(`${logPrefix} TTS: Wyoming synthesis took ${synthesisDurationMs.toFixed(2)} ms. Received ${rawPcmAudioBuffer.length} bytes of PCM data.`);

					if (rawPcmAudioBuffer.length === 0) {
						// Don't throw an error, but maybe warn and return item without audio? Or handle based on a setting?
						logger.warn(`${logPrefix} TTS: Synthesis resulted in empty audio data. Returning item without audio in field "${outputFieldName}".`);
						// Create item without the binary data
                         newItem = { json: JSON.parse(JSON.stringify(item.json)), pairedItem: { item: itemIndex } };
					} else {
						// --- Add WAV Header ---
						const wavBuffer = _addWavHeader(rawPcmAudioBuffer, SAMPLE_RATE, SAMPLE_WIDTH, SAMPLE_CHANNELS);
						logger.info(`${logPrefix} TTS: Added WAV header. Total WAV size: ${wavBuffer.length} bytes.`);

						// --- Prepare TTS Output ---
						const outputFileName = `tts_output_${itemIndex}.wav`; // Generate a filename
						const binaryOutputData: IBinaryData = await this.helpers.prepareBinaryData(wavBuffer, outputFileName, DEFAULT_TTS_OUTPUT_MIME_TYPE);

						// Create a new item structure for the output, preserving input JSON but adding binary data
						newItem = {
							json: JSON.parse(JSON.stringify(item.json)), // Keep original JSON data
							binary: { // Add the new binary data
								[outputFieldName]: binaryOutputData,
							},
							pairedItem: { item: itemIndex },
						};
					}

					const overallEndTime = performance.now();
					logger.info(`${logPrefix} TTS: Successfully processed item in ${(overallEndTime - overallStartTime).toFixed(2)} ms (Synthesis: ${synthesisDurationMs.toFixed(2)} ms).`);
				}

				// Add the successfully processed item to the return data
                if (newItem) {
				    returnData.push(newItem);
                } else {
                    // This case should ideally not happen if logic above is correct
                    logger.warn(`${logPrefix} No output item was generated for an unknown reason. Skipping item.`);
                    // Optionally push original item if continue on fail?
                }

			} catch (error: any) {
				// --- Refined Error Handling (Common for STT/TTS) ---
				const errorMessage = error.message || String(error);
				const originalCode = (error as any).originalCode; // Socket error code
				const isSpawnError = (error as any).isSpawnError; // FFmpeg spawn error (STT only)
				const isConfigError = (error as any).isConfigurationError; // Bad address/port
				logger.error(`${logPrefix} Error processing item (${operation.toUpperCase()}): ${errorMessage}`, error);
				const overallEndTime = performance.now();
				logger.debug(`${logPrefix} Failed processing item after ${(overallEndTime - overallStartTime).toFixed(2)} ms.`);

				// Determine if the error is critical (stop workflow regardless of continueOnFail)
				// Critical: Cannot connect, cannot find ffmpeg
				const isCriticalNetworkError = ['ECONNREFUSED', 'ENOTFOUND', 'EHOSTUNREACH', 'EAI_AGAIN'].includes(originalCode) || isConfigError;
				const isCriticalError = isCriticalNetworkError || isSpawnError;

				if (isCriticalError) {
					logger.error(`${logPrefix} Critical error detected (${originalCode || (isSpawnError ? 'ffmpeg spawn failed' : 'config error')}). Stopping workflow execution.`);
					// Ensure the error is thrown correctly to stop the workflow
					if (error instanceof NodeOperationError) throw error;
					throw new NodeOperationError(this.getNode(), error as Error, { itemIndex, description: errorMessage });
				} else {
					// For non-critical errors (timeouts, server errors, ffmpeg *conversion* issues, synthesis errors, etc.), respect "Continue on Fail"
					if (this.continueOnFail()) {
						logger.warn(`${logPrefix} Non-critical error occurred. continueOnFail is true. Recording error and continuing workflow.`);
						// Ensure error is an instance of NodeOperationError before attaching to item
						const n8nError = error instanceof NodeOperationError ? error : new NodeOperationError(this.getNode(), error as Error, { itemIndex, description: errorMessage });
						// Return the original item with the error attached
						const errorItem: INodeExecutionData = {
							json: item.json, // Keep original JSON
							binary: item.binary, // Keep original binary data (if any)
							error: n8nError, // Attach the error object
							pairedItem: { item: itemIndex },
						};
						returnData.push(errorItem);
					} else {
						// If continueOnFail is false, stop the workflow by re-throwing
						logger.warn(`${logPrefix} Non-critical error occurred. continueOnFail is false. Stopping workflow execution.`);
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
