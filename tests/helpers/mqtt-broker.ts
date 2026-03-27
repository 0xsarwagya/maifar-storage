import { Aedes } from "aedes";
import { createServer, type AddressInfo } from "node:net";

export type BrokerFixture = {
  port: number;
  mqttUrl: string;
  close: () => Promise<void>;
};

export async function startEmbeddedMqttBroker(): Promise<BrokerFixture> {
  const broker = await Aedes.createBroker({ drainTimeout: 0 });
  const server = createServer(broker.handle);

  await new Promise<void>((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => resolve());
    server.on("error", reject);
  });

  const addr = server.address() as AddressInfo;
  const port = addr.port;
  const mqttUrl = `mqtt://127.0.0.1:${port}`;

  return {
    port,
    mqttUrl,
    close: () =>
      new Promise<void>((resolve, reject) => {
        broker.close(() => {
          server.close((err) => (err ? reject(err) : resolve()));
        });
      }),
  };
}
