import { HttpClient, HttpRequest, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BASE_URL } from '../app.config';
import { IJar } from '../interfaces/jar';
import { INode } from '../interfaces/job';

@Injectable({
  providedIn: 'root'
})
export class JarService {

  loadJarList() {
    return this.httpClient.get<IJar>(`${BASE_URL}/jars`);
  }

  uploadJar(fd) {
    const formData = new FormData();
    formData.append('jarfile', fd, fd.name);
    console.log(formData);
    const req = new HttpRequest('POST', `${BASE_URL}/jars/upload`, formData, {
      reportProgress: true
    });
    return this.httpClient.request(req);
  }

  deleteJar(jarId) {
    return this.httpClient.delete(`${BASE_URL}/jars/${jarId}`);
  }

  runJob(jarId, params) {
    return this.httpClient.post<{ jobid: string }>(`${BASE_URL}/jars/${jarId}/run`, params);
  }

  getPlan(jarId, entryClass, parallelism, programArgs) {
    let params = new HttpParams();
    if (entryClass) {
      params = params.append('entry-class', entryClass);
    }
    if (parallelism) {
      params = params.append('parallelism', parallelism);
    }
    if (programArgs) {
      params = params.append('program-args', programArgs);
    }
    return this.httpClient.get<{
      'plan': {
        jid: string;
        name: string;
        nodes: INode[];
      }
    }>(`${BASE_URL}/jars/${jarId}/plan`, {
      params: params
    });
  }

  constructor(private httpClient: HttpClient) {
  }
}
