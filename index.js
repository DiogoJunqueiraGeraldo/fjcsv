const fs = require('fs');
const { promisify } = require('util')
const { pipeline } = require('stream');

const INPUT_FILE_PATH = './source.json';
const OUTPUT_FILE_PATH = './output.csv';
const OBJECT_START_TOKEN = '{';
const OBJECT_END_TOKEN = '}';
const IGNORE_REGEX = /\[|\]|\n|\\n|\\r/g;
const SEPARATOR = ';'

const pipelineAsync = promisify(pipeline);

async function run() {
	let headers = false;

	await pipelineAsync(
		fs.createReadStream(INPUT_FILE_PATH),
		async function* (data) {
			data.setEncoding('utf8');

			let lastChunk = '';
			for await (const chunk of data) {
				let buff = lastChunk + chunk;
				let objs = buff.replace(IGNORE_REGEX, '');
				const buffLength = buff.length;

				while (true) {
					let start = objs.indexOf(OBJECT_START_TOKEN);
					let end = objs.indexOf(OBJECT_END_TOKEN);

					if (start > 0 && end > 0) {
						let inclusiveEnd = end + 1;

						let obj = JSON.parse(objs.slice(start, inclusiveEnd));

						if (!headers) {
							yield [...Object.keys(obj), '\n'].join(SEPARATOR);
							headers = true;
						}

						yield [...Object.values(obj), '\n'].join(SEPARATOR);

						objs = objs.slice(inclusiveEnd, buffLength);
					} else {
						lastChunk = objs;
						break;
					}
				}
			}
		},
		fs.createWriteStream(OUTPUT_FILE_PATH)
	)
}

let start = new Date().getTime();

run()
	.then(() => {
		console.log(`Execution Time: ${new Date().getTime() - start}`);
		console.log(`Done! Take a look at ${OUTPUT_FILE_PATH}`);
	})
	.catch((err) => {
		console.error(err);
	});