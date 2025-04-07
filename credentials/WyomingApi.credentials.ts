
import {
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class WyomingApi implements ICredentialType {
	name = 'wyomingApi';
	displayName = 'Wyoming API';
	properties: INodeProperties[] = [
		{
			displayName: 'Host',
			name: 'host',
			type: 'string',
			default: '127.0.0.1',
		},
		{
			displayName: 'Port',
			name: 'port',
			type: 'number',
			default: 10300,
		},
	];
}

export type WyomingCredentials = {
	host: string;
	port: number;
}
