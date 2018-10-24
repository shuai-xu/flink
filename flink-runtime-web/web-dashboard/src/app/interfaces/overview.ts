export interface IOverview {
  'flink-commit': string;
  'flink-version': string;
  'jobs-cancelled': number;
  'jobs-failed': number;
  'jobs-finished': number;
  'jobs-running': number;
  'slots-available': number;
  'slots-total': number;
  'taskmanagers': number;
}
