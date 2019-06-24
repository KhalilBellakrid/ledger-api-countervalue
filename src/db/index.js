// @flow

import mongodb from "./mongodb";
import postgres from "./postgres";
import type { Database } from "../types";

const databases: { [_: string]: Database } = {
  mongodb,
  postgres
};

export const getCurrentDatabase = (): Database => {
  const key = process.env.DATABASE || "mongodb";
  const db = databases[key];
  if (!db) {
    throw new Error(`database '${key}' not found`);
  }
  return db;
};
