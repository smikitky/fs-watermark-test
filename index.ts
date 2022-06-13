import * as fsp from "fs/promises";
import * as fs from "fs";
import * as s from "stream";
import * as path from "path";

// Reports `writableLength` of the given buffer until the stream finishes
const checkStream = (stream: s.Writable) => {
  return new Promise<void>((resolve) => {
    const id = setInterval(() => {
      const length = stream.writableLength;
      console.log(length, new Date().toISOString());
    }, 100);
    stream.on("finish", () => {
      clearInterval(id);
      resolve();
    });
  });
};

interface Options {
  chunkSize: number;
  totalSize?: number;
  waitForDrain?: boolean;
  reportStatus?: boolean;
}

// asynchronously write large data to this directory
const test = async ({
  chunkSize,
  totalSize = 1 * 1024 * 1024 * 1024,
  waitForDrain = false,
  reportStatus = false,
}: Options) => {
  if (totalSize % chunkSize !== 0) {
    console.error("totalSize is not divisible by chunkSize");
    return;
  }

  const fileName = path.join(__dirname, "test.data");
  const stream = fs.createWriteStream(fileName);

  let handler: () => void;
  stream.on("drain", () => handler());
  const wait = () => new Promise<void>((resolve) => (handler = resolve));

  const start = Date.now();

  await Promise.all([
    (async () => {
      for (let i = 0; i < totalSize / chunkSize; i++) {
        const okay = stream.write(Buffer.alloc(chunkSize));
        if (waitForDrain && !okay) await wait();
      }
      stream.end();
    })(),
    ...(reportStatus ? [checkStream(stream)] : []),
  ]);

  const end = Date.now();
  await fsp.unlink(fileName);
  const time = end - start;

  console.log({ chunkSize, waitForDrain, time });
};

const main = async () => {
  const chunkSize = 4 * 1024;
  await test({ chunkSize, waitForDrain: false, reportStatus: true });
  await test({ chunkSize, waitForDrain: true, reportStatus: true });
};

main();
