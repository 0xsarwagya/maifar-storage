import { afterEach } from "bun:test";
import { resetQueue } from "../src/queue";

afterEach(() => {
  resetQueue();
});
