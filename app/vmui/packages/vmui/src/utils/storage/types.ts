import { ALL_STORAGE_KEYS, STORAGE_PREFIX, TABLE_KEYS } from "./constants";

type TableKeyPrefix = (typeof TABLE_KEYS)[keyof typeof TABLE_KEYS];
export type TableStorageKey = `${TableKeyPrefix}:${string}`;

export type StorageKeys = (typeof ALL_STORAGE_KEYS)[number] | TableStorageKey;
export type PrefixedStorageKeys = `${typeof STORAGE_PREFIX}${StorageKeys}`;

export type StorageValue = string | boolean | Record<string, unknown> | unknown[];

