
export interface IJob {
  'jid': string;
  'start-time': number;
  'end-time': number;
  'duration': number;
  'last-modification': number;
  'name': string;
  'state': string;
  'tasks': IJobTask;
  'completed'?: boolean;
}

export interface IJobTask {
  'CANCELED': number;
  'CANCELING': number;
  'CREATED': number;
  'DEPLOYING': number;
  'FAILED': number;
  'FINISHED': number;
  'RECONCILING': number;
  'RUNNING': number;
  'SCHEDULED': number;
  'TOTAL': number;
}

export interface IJobDetailCorrect extends IJobDetail {
  'plan': {
    jid: string;
    name: string;
    nodes: INodeCorrect[];
    links: INodeLink[];
  };
}

export interface IJobStatusCounts {
  'CREATED': number;
  'SCHEDULED': number;
  'CANCELED': 17;
  'DEPLOYING': number;
  'RUNNING': number;
  'CANCELING': number;
  'FINISHED': number;
  'FAILED': number;
  'RECONCILING': number;
}

export interface IJobDetail {
  'jid': string;
  'name': string;
  'isStoppable': boolean;
  'state': string;
  'start-time': number;
  'end-time': number;
  'duration': number;
  'now': number;
  'timestamps': {
    'CREATED': number;
    'RUNNING': number;
    'FAILING': number;
    'RECONCILING': number;
    'CANCELLING': number;
    'RESTARTING': number;
    'FINISHED': number;
    'FAILED': number;
    'CANCELED': number;
    'SUSPENDED': number;
  };
  'vertices': IVertex[];
  'status-counts': IJobStatusCounts;
  'plan': {
    jid: string;
    name: string;
    nodes: INode[];
  };
}

export interface IVertex {
  'id': string;
  'name': string;
  'parallelism': number;
  'status': string;
  'start-time': number;
  'end-time': number;
  'duration': number;
  'tasks': {
    'CREATED': number;
    'SCHEDULED': number;
    'CANCELED': 1,
    'DEPLOYING': number;
    'RUNNING': number;
    'CANCELING': number;
    'FINISHED': number;
    'FAILED': number;
    'RECONCILING': number;
  };
  'metrics': {
    'read-bytes': number;
    'read-bytes-complete': boolean;
    'read-records': number;
    'read-records-complete': boolean;
    'write-bytes': number;
    'write-bytes-complete': boolean;
    'write-records': number;
    'write-records-complete': boolean;
    'tps_avg'?: number;
    'tps_max'?: number;
    'tps_min'?: number;
    'tps_sum'?: number;
    'buffers.inPoolUsage_avg'?: number;
    'buffers.inPoolUsage_max'?: number;
    'buffers.inPoolUsage_min'?: number;
    'buffers.inPoolUsage_sum'?: number;
    'buffers.inputQueueLength_avg'?: number;
    'buffers.inputQueueLength_max'?: number;
    'buffers.inputQueueLength_min'?: number;
    'buffers.inputQueueLength_sum'?: number;
    'buffers.outPoolUsage_avg'?: number;
    'buffers.outPoolUsage_max'?: number;
    'buffers.outPoolUsage_min'?: number;
    'buffers.outPoolUsage_sum'?: number;
    'buffers.outputQueueLength_avg'?: number;
    'buffers.outputQueueLength_max'?: number;
    'buffers.outputQueueLength_min'?: number;
    'buffers.outputQueueLength_sum'?: number;
    'delay_avg'?: number;
    'delay_max'?: number;
    'delay_min'?: number;
    'delay_sum'?: number;
    'latency_avg'?: number;
    'latency_max'?: number;
    'latency_min'?: number;
    'latency_sum'?: number;
    'numRecordsInPerSecond_avg'?: number;
    'numRecordsInPerSecond_max'?: number;
    'numRecordsInPerSecond_min'?: number;
    'numRecordsInPerSecond_sum'?: number;
    'numRecordsIn_avg'?: number;
    'numRecordsIn_max'?: number;
    'numRecordsIn_min'?: number;
    'numRecordsIn_sum'?: number;
    'numRecordsOut_avg'?: number;
    'numRecordsOut_max'?: number;
    'numRecordsOut_min'?: number;
    'numRecordsOut_sum'?: number;
  };
  'topology-id'?: number;
  'vcore'?: number;
  'memory'?: number;
}

export interface INodeCorrect extends INode {
  detail: IVertex;
}

export interface INode {
  'id': string;
  'parallelism': number;
  'operator': string;
  'operator_strategy': string;
  'description': string;
  'inputs'?: INodeInput[];
  'optimizer_properties': {};
  width?: number;
  height?: number;
}

export interface INodeLink {
  id: string;
  source: string;
  target: string;
  detail: INodeInput;
}

export interface INodeInput {
  'num': number;
  'id': string;
  'ship_strategy': string;
  'exchange': string;
}

export interface IJobConfig {
  'jid': string;
  'name': string;
  'execution-config': {
    'execution-mode': string;
    'restart-strategy': string;
    'job-parallelism': number;
    'object-reuse-mode': boolean;
    'user-config': {
      [ key: string ]: string;
    }
  };
}

export interface IJobSubTaskTime {
  'id': string;
  'name': string;
  'now': number;
  'subtasks': Array<{
    'subtask': number;
    'host': string;
    'duration': number;
    'timestamps': {
      'CREATED': number;
      'RUNNING': number;
      'FAILING': number;
      'RECONCILING': number;
      'CANCELLING': number;
      'RESTARTING': number;
      'FINISHED': number;
      'FAILED': number;
      'CANCELED': number;
      'SUSPENDED': number;
    }
  }>;
}

export interface IException {
  'root-exception': string;
  timestamp: number;
  truncated: boolean;
  'all-exceptions': Array<{
    'attempt-num': number;
    'exception': string;
    'location': string;
    'subtask-index': number;
    'task': string;
    'timestamp': number;
    'vertex-id': string;
  }>;
}

export interface ICheckPoint {
  counts: {
    'restored': number;
    'total': number;
    'in_progress': number;
    'completed': number;
    'failed': number
  };
  summary: {
    'state_size': ICheckPointMinMaxAvgStatistics;
    'end_to_end_duration': ICheckPointMinMaxAvgStatistics;
    'alignment_buffered': ICheckPointMinMaxAvgStatistics;
  };
  latest: {
    completed: ICheckPointCompletedStatistics;
    savepoint: ICheckPointCompletedStatistics;
    failed: {
      'id': number;
      'status': string;
      'is_savepoint': boolean;
      'trigger_timestamp': number;
      'latest_ack_timestamp': number;
      'state_size': number;
      'end_to_end_duration': number;
      'alignment_buffered': number;
      'num_subtasks': number;
      'num_acknowledged_subtasks': number;
      'failure_timestamp': number;
      'failure_message': string;
      task: ICheckPointTaskStatistics;
    };
    restored: {
      'id': number;
      'restore_timestamp': number;
      'is_savepoint': boolean;
      'external_path': string;
    };
    history: {
      'id': number;
      'status': string;
      'is_savepoint': boolean;
      'trigger_timestamp': number;
      'latest_ack_timestamp': number;
      'state_size': number;
      'end_to_end_duration': number;
      'alignment_buffered': number;
      'num_subtasks': number;
      'num_acknowledged_subtasks': number;
      task: ICheckPointTaskStatistics;
    }
  };
}

export interface ICheckPointMinMaxAvgStatistics {
  'min': number;
  'max': number;
  'avg': number;
}

export interface ICheckPointCompletedStatistics {
  'id': number;
  'status': string;
  'is_savepoint': boolean;
  'trigger_timestamp': number;
  'latest_ack_timestamp': number;
  'state_size': number;
  'end_to_end_duration': number;
  'alignment_buffered': number;
  'num_subtasks': number;
  'num_acknowledged_subtasks': number;
  tasks: ICheckPointTaskStatistics;
  external_path: string;
  discarded: boolean;
}

export interface ICheckPointTaskStatistics {
  'id': number;
  'status': string;
  'latest_ack_timestamp': number;
  'state_size': number;
  'end_to_end_duration': number;
  'alignment_buffered': number;
  'num_subtasks': number;
  'num_acknowledged_subtasks': number;
}

export interface ICheckPointConfig {
  'mode': any;
  'interval': number;
  'timeout': number;
  'min_pause': number;
  'max_concurrent': number;
  'externalization': {
    'enabled': boolean;
    'delete_on_cancellation': boolean;
  };
}

export interface ICheckPointDetail {
  'id': number;
  'status': string;
  'is_savepoint': boolean;
  'trigger_timestamp': number;
  'latest_ack_timestamp': number;
  'state_size': number;
  'end_to_end_duration': number;
  'alignment_buffered': number;
  'num_subtasks': number;
  'num_acknowledged_subtasks': number;
  'tasks': Array<{
    [ taskId: string ]: {
      'id': number;
      'status': string;
      'latest_ack_timestamp': number;
      'state_size': number;
      'end_to_end_duration': number;
      'alignment_buffered': number;
      'num_subtasks': number;
      'num_acknowledged_subtasks': number;
    }
  }>;
}

export interface ICheckPointSubTask {
  'id': number;
  'status': string;
  'latest_ack_timestamp': number;
  'state_size': number;
  'end_to_end_duration': number;
  'alignment_buffered': number;
  'num_subtasks': number;
  'num_acknowledged_subtasks': number;
  'summary': {
    'state_size': ICheckPointMinMaxAvgStatistics;
    'end_to_end_duration': ICheckPointMinMaxAvgStatistics;
    'checkpoint_duration': {
      'sync': ICheckPointMinMaxAvgStatistics
      'async': ICheckPointMinMaxAvgStatistics
    },
    'alignment': {
      'buffered': ICheckPointMinMaxAvgStatistics
      'duration': ICheckPointMinMaxAvgStatistics
    }
  };
  'subtasks': Array<{
    'index': number;
    'status': string;
  }>;
}


export interface IVertexTaskManager {
  'id': string;
  'name': string;
  'now': number;
  'taskmanagers': Array<IVertexTaskManagerDetail>;
}

export interface IVertexTaskManagerDetail {
  'host': string;
  'status': string;
  'start-time': number;
  'end-time': number;
  'duration': number;
  'metrics': {
    'read-bytes': number;
    'read-bytes-complete': boolean;
    'write-bytes': number;
    'write-bytes-complete': boolean;
    'read-records': number;
    'read-records-complete': boolean;
    'write-records': number;
    'write-records-complete': boolean;
  };
  'status-counts': {
    CANCELED: number;
    CANCELING: number;
    CREATED: number;
    DEPLOYING: number;
    FAILED: number;
    FINISHED: number;
    RECONCILING: number;
    RUNNING: number;
    SCHEDULED: number;
  };
}


export interface ISubTask {
  'attempt': number;
  'duration': number;
  'end-time': number;
  'host': string;
  'start_time': number; // TODO change to correct
  'status': string;
  'subtask': number;
  'metrics': {
    'read-bytes': number;
    'read-bytes-complete': boolean;
    'read-records': number
    'read-records-complete': boolean;
    'write-bytes': number;
    'write-bytes-complete': boolean;
    'write-records': number;
    'write-records-complete': boolean;
    'buffers.inPoolUsage'?: string;
    'buffers.inputQueueLength'?: string;
    'buffers.outPoolUsage'?: string;
    'buffers.outputQueueLength'?: string;
    'delay'?: string;
    'latency'?: string;
    'tps'?: string;
    'numRecordsInPerSecond'?: string;
  };
  'dataPort'?: number;
}
