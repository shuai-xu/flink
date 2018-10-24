export interface ITaskManager {
  dataPort: number;
  freeSlots: number;
  hardware: {
    cpuCores: number;
    physicalMemory: number;
    freeMemory: number;
    managedMemory: number;
  };
  cpuCores: number;
  freeMemory: number;
  managedMemory: number;
  physicalMemory: number;
  host: string;
  id: string;
  path: string;
  slotsNumber: number;
  timeSinceLastHeartbeat: number;
}

export interface ITaskManagerDetail {
  'id': string;
  'path': string;
  'dataPort': number;
  'timeSinceLastHeartbeat': number;
  'slotsNumber': number;
  'freeSlots': number;
  'hardware': {
    'cpuCores': number;
    'physicalMemory': number;
    'freeMemory': number;
    'managedMemory': number;
  };
  'metrics': {
    'heapUsed': number;
    'heapCommitted': number;
    'heapMax': number;
    'nonHeapUsed': number;
    'nonHeapCommitted': number;
    'nonHeapMax': number;
    'directCount': number;
    'directUsed': number;
    'directMax': number;
    'mappedCount': number;
    'mappedUsed': number;
    'mappedMax': number;
    'memorySegmentsAvailable': number;
    'memorySegmentsTotal': number;
    'garbageCollectors': Array<{
      'name': string;
      'count': number;
      'time': number;
    }>;
  };
}
