import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BASE_URL } from '../app.config';
import { IOverview } from '../interfaces/overview';

@Injectable({
  providedIn: 'root'
})
export class OverviewService {

  constructor(private httpClient: HttpClient) {
  }

  loadOverview() {
    return this.httpClient.get<IOverview>(`${BASE_URL}/overview`);
  }
}
