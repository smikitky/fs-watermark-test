import * as fsp from "fs/promises";
import * as fs from "fs";
import * as s from "stream";
import * as path from "path";

// Reports `writableLength` of the given buffer until the stream finishes
const checkStream = (stream: s.Writable, report: boolean) => {
  return new Promise<void>((resolve) => {
    const id = setInterval(() => {
      const length = stream.writableLength;
      report && console.log(length, new Date().toISOString());
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

  const startedAt = Date.now();
  let streamWrittenAt: number;

  await Promise.all([
    (async () => {
      for (let i = 0; i < totalSize / chunkSize; i++) {
        const okay = stream.write(Buffer.alloc(chunkSize));
        if (waitForDrain && !okay) await wait();
      }
      stream.end();
      streamWrittenAt = Date.now();
    })(),
    checkStream(stream, reportStatus),
  ]);

  const streamClosedAt = Date.now();
  await fsp.unlink(fileName);

  // time between createWriteStream and stream.on("finish")
  const totalTime = streamClosedAt - startedAt;

  // time between stream.end() and stream.on("finish")
  const lagTime = streamClosedAt - streamWrittenAt!;

  console.log({ chunkSize, waitForDrain, totalTime, lagTime });
};

const main = async () => {
  const huge = 1024 * 1024 * 1024; // 1GB
  const large = 64 * 1024 * 1024; // 64MB
  const middle = 4 * 1024 * 1024; // 4MB
  const small = 4 * 1024; // 4KB

  await test({ chunkSize: large, waitForDrain: false, reportStatus: true });
  await test({ chunkSize: large, waitForDrain: true, reportStatus: true });

  // await test({ chunkSize: huge, waitForDrain: true });
  // await test({ chunkSize: large, waitForDrain: false, reportStatus: true });
  // await test({ chunkSize: large, waitForDrain: true, reportStatus: true });
  // await test({ chunkSize: small, waitForDrain: false });
  // await test({ chunkSize: small, waitForDrain: true });
};

main();
