import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { NavigationEnd, Router } from '@angular/router';
import { EMPTY, fromEvent, interval, merge, Subject } from 'rxjs';
import { debounceTime, filter, mapTo, startWith, switchMap, tap } from 'rxjs/operators';
import { BASE_URL } from '../app.config';
import { IConfiguration } from '../interfaces/configuration';

@Injectable({
  providedIn: 'root'
})
export class StatusService {
  // collapsed flag
  isCollapsed = false;
  // loading flag
  isLoading = false;
  // flink configuration
  configuration: IConfiguration;
  // refresh stream
  refresh$ = new Subject<boolean>().asObservable();
  // manual refresh stream
  private manual$ = new Subject<boolean>();
  // focus & blur stream
  private focus$ = merge(fromEvent(window, 'focus').pipe(mapTo(true)), fromEvent(window, 'blur').pipe(mapTo(false)));

  manualRefresh() {
    this.manual$.next(true);
  }

  /** init flink config before booting **/
  boot(router: Router): Promise<IConfiguration> {
    this.isLoading = true;
    return this.httpClient.get<IConfiguration>(`${BASE_URL}/config`).pipe(tap((data) => {
      this.configuration = data;
      const navigationEnd$ = router.events.pipe(filter(item => (item instanceof NavigationEnd)), mapTo(true));
      const interval$ = interval(this.configuration[ 'refresh-interval' ]).pipe(mapTo(true), startWith(true));
      this.refresh$ = merge(this.focus$, this.manual$, navigationEnd$).pipe(
        startWith(true),
        debounceTime(300),
        switchMap(active => active ? interval$ : EMPTY)
      );
      this.isLoading = false;
    })).toPromise();
  }

  constructor(private httpClient: HttpClient) {
  }
}
