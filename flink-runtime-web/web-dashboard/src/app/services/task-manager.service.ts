import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';
import { map } from 'rxjs/operators';
import { BASE_URL } from '../app.config';
import { ITaskManager, ITaskManagerDetail } from '../interfaces/task-manager';

@Injectable({
  providedIn: 'root'
})
export class TaskManagerService {
  taskManagerDetail: ITaskManagerDetail;
  taskManagerDetail$ = new Subject<ITaskManagerDetail>();

  loadManagers() {
    return this.httpClient.get<{ taskmanagers: ITaskManager[] }>(`${BASE_URL}/taskmanagers`).pipe(map(data => data.taskmanagers || []));
  }

  loadManager(taskManagerId) {
    return this.httpClient.get<ITaskManagerDetail>(`${BASE_URL}/taskmanagers/${taskManagerId}`);
  }

  loadLogs(taskManagerId) {
    return this.httpClient.get(`${BASE_URL}/taskmanagers/${taskManagerId}/log`, { responseType: 'text' });
  }

  loadStdout(taskManagerId) {
    return this.httpClient.get(`${BASE_URL}/taskmanagers/${taskManagerId}/stdout`, { responseType: 'text' });
  }

  constructor(private httpClient: HttpClient) {
  }
}
