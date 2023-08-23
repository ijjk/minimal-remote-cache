const fs = require("fs");
const path = require("path");
const http = require("http");
const { Sema } = require("./vendored/async-sema");

async function main() {
  const {
    STORAGE_DIR = "remote-cache",
    PORT = "3939",
    TURBO_TOKEN,
    CACHE_DAYS = "7",
    CLEANUP_MINUTES = "5",
  } = process.env;

  if (!TURBO_TOKEN) {
    throw new Error(`TURBO_TOKEN env is required please set to continue`);
  }

  const cacheDays = Number(CACHE_DAYS);
  const storageDir = path.resolve(STORAGE_DIR);
  const cacheEntries = new Map();

  console.log("Using storageDir", storageDir);

  {
    await fs.promises.mkdir(storageDir, { recursive: true });
    const files = await fs.promises.readdir(storageDir);
    const collectSema = new Sema(25, { capacity: files.length });
    await Promise.all(
      files.map(async (file) => {
        try {
          await collectSema.acquire();
          const statData = await fs.promises.stat(path.join(storageDir, file));

          if (statData.isFile()) {
            cacheEntries.set(file, {
              lastModified: statData.mtimeMs,
            });
          }
        } finally {
          collectSema.release();
        }
      })
    );
  }
  let lastCleanup = Date.now();
  let cleaningActive = false;
  const cleanupMinutes = Number(CLEANUP_MINUTES);

  async function cleanupEntries() {
    // only allow one running cleanup at a time
    // and only cleanup every 5 minutes
    if (
      cleaningActive ||
      lastCleanup > Date.now() - cleanupMinutes * 60 * 1000
    ) {
      return;
    }
    console.log("Cleaning up old artifacts");

    cleaningActive = true;
    const sevenDaysAgo = Date.now() - cacheDays * 24 * 60 * 60 * 1000;

    for (const [key, value] of cacheEntries.entries()) {
      try {
        if (value.lastModified < sevenDaysAgo) {
          console.log(
            `Cleaning up old artifact ${key}, lastModified: ${value.lastModified}`
          );
          cacheEntries.delete(key);
          await fs.promises.unlink(path.join(storageDir, key));
        }
      } catch (err) {
        console.error(`Failed to clean up artifact ${key}`, err);
      }
    }
    cleaningActive = false;
    lastCleanup = Date.now();
  }
  await cleanupEntries();

  const server = http.createServer(async (req, res) => {
    try {
      await cleanupEntries();
      // authorize request with token
      if (req.headers["authorization"] !== `Bearer ${TURBO_TOKEN}`) {
        console.log(`Forbidden for ${req.socket.remoteAddress} ${req.url}`);
        res.statusCode = 403;
        res.end("Forbidden");
        return;
      }
      const parsedUrl = new URL(req.url || "/", "http://n");
      const artifactsPath = "/v8/artifacts/";

      if (parsedUrl.pathname.startsWith(artifactsPath)) {
        if (req.method === "OPTIONS") {
          res.setHeader("Access-Control-Allow-Headers", "Authorization");
          res.setHeader(
            "Access-Control-Allow-Methods",
            "GET, PUT, HEAD, OPTIONS"
          );
          res.statusCode = 200;
          res.end("");
          return;
        }
        const artifactName = parsedUrl.pathname.substring(artifactsPath.length);

        if (req.method === "GET" || req.method === "HEAD") {
          console.log(
            req.method,
            "-",
            req.url,
            cacheEntries.has(artifactName) ? "HIT" : "MISS"
          );

          if (cacheEntries.has(artifactName)) {
            if (req.method === "HEAD") {
              res.statusCode = 200;
              res.end("");
            } else {
              fs.createReadStream(path.join(storageDir, artifactName))
                .pipe(res)
                .on("error", (err) => {
                  console.log(`Streaming artifact ${artifactName} failed`, err);
                  res.end("");
                });
            }
            return;
          }
        }

        if (req.method === "PUT") {
          console.log(req.method, '-', req.url);
          await new Promise((resolve, reject) => {
            const outputPath = path.join(storageDir, artifactName);
            const writeStream = fs.createWriteStream(outputPath);
            writeStream.on("error", (err) => {
              console.log(`Failed to set artifact ${artifactName}`, err);
              res.end("");

              try {
                fs.unlinkSync(outputPath);
              } catch (_) {
                // attempted cleanup
              }
              reject();
            });
            writeStream.on("close", () => {
              res.end("");
              resolve();
            });
            req.pipe(writeStream);
          });
          cacheEntries.set(artifactName, {
            lastModified: Date.now(),
          });
          return;
        }
      }
      res.statusCode = 404;
      res.end("Not Found");
    } catch (err) {
      console.error(err);
      res.statusCode = 500;
      res.end("Internal Server Error");
    }
  });

  const port = Number(PORT);

  await new Promise((resolve, reject) => {
    server.listen(port, "0.0.0.0", (err) => {
      if (err) {
        return reject(err);
      }
      resolve();
      console.log(`Remote cache listening at http://localhost:${port}`);
    });
  });
}

main().catch(console.error);
