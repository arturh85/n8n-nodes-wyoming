# n8n-nodes-wyoming

This is an n8n community node. It lets you use [wyoming](https://github.com/rhasspy/wyoming) compatible servers for STT in your n8n workflows.

Wyoming is a peer-to-peer protocol for voice assistants (basically JSONL + PCM audio)

[n8n](https://n8n.io/) is a [fair-code licensed](https://docs.n8n.io/reference/license/) workflow automation platform.

[Installation](#installation)  
[Operations](#operations)  
[Compatibility](#compatibility)  
[Usage](#usage)
[Resources](#resources)  

## Installation

Follow the [installation guide](https://docs.n8n.io/integrations/community-nodes/installation/) in the n8n community nodes documentation.

## Operations

- Transcribe Audio

## Compatibility

- Tested with Version 1.85.4

## Usage

- Set the Wyoming Server URL and language
- If the input is not in PCM format already conversion with [ffmpeg](https://ffmpeg.org/) is attempted.

## Resources

* [n8n community nodes documentation](https://docs.n8n.io/integrations/community-nodes/)
