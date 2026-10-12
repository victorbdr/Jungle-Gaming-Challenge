import { v7 as uuidv7 } from 'uuid';

/** UUIDv7: ordenável no tempo, bom para índices B-tree e cursores. */
export const newId = (): string => uuidv7();
