import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BASE_URL } from '../app.config';

@Injectable({
  providedIn: 'root'
})
export class JobManagerService {

  loadConfig() {
    return this.httpClient.get<Array<{ key: string; value: string; }>>(`${BASE_URL}/jobmanager/config`);
  }

  loadLogs(page = -1, count = 10240) {
    const start = page * count;
    const params = new HttpParams().append('start', `${start}`).append('count', `${count}`);
    return this.httpClient.get(`${BASE_URL}/jobmanager/log`, { params: params, responseType: 'text' });
  }

  loadStdout(page = -1, count = 10240) {
    const start = page * count;
    const params = new HttpParams().append('start', `${start}`).append('count', `${count}`);
    return this.httpClient.get(`${BASE_URL}/jobmanager/stdout`, { params: params, responseType: 'text' });
  }

  constructor(private httpClient: HttpClient) {
  }
}
