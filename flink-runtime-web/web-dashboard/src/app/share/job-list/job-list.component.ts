import { Component, EventEmitter, Input, OnDestroy, OnInit, Output } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { IJob, INodeCorrect } from '../../interfaces/job';
import { JobService } from '../../services/job.service';
import { StatusService } from '../../services/status.service';
import { isNil } from 'lodash';

@Component({
  selector   : 'flink-job-list',
  templateUrl: './job-list.component.html',
  styleUrls  : [ './job-list.component.less' ]
})
export class JobListComponent implements OnInit, OnDestroy {
  listOfJob: IJob[] = [];
  isLoading = true;
  destroy$ = new Subject();
  sortName = 'start-time';
  sortValue = 'descend';
  @Input() completed = false;
  @Input() title: string;
  @Input() jobData$;

  sort(sort: { key: string, value: string }) {
    this.sortName = sort.key;
    this.sortValue = sort.value;
    this.search();
  }

  search() {
    if (this.sortName) {
      this.listOfJob = [ ...this.listOfJob.sort(
        (pre, next) => {
          if (this.sortValue === 'ascend') {
            return (this.deepFind(pre, this.sortName) > this.deepFind(next, this.sortName) ? 1 : -1);
          } else {
            return (this.deepFind(next, this.sortName) > this.deepFind(pre, this.sortName) ? 1 : -1);
          }
        }) ];
    }
  }

  deepFind(obj, path) {
    const paths = path.split('.');
    let current = obj;
    for (let i = 0; i < paths.length; ++i) {
      const rePath = paths[ i ].replace('$', '.');
      if (current[ rePath ] === undefined) {
        return undefined;
      } else {
        current = current[ rePath ];
      }
    }
    return current;
  }


  trackJobBy(index, node) {
    return node.jid;
  }

  navigateToJob(jid) {
    this.router.navigate([ 'job', jid ]).then();
  }

  constructor(
    private statusService: StatusService,
    private jobService: JobService,
    private activatedRoute: ActivatedRoute,
    private router: Router) {
  }

  ngOnInit() {
    if (this.activatedRoute.snapshot.data) {
      this.completed = isNil(this.activatedRoute.snapshot.data.completed) ? this.completed : this.activatedRoute.snapshot.data.completed;
      this.title = isNil(this.activatedRoute.snapshot.data.title) ? this.title : this.activatedRoute.snapshot.data.title;
    }
    this.jobData$ = this.jobData$ || this.statusService.refresh$.pipe(
      takeUntil(this.destroy$),
      flatMap(() => this.jobService.loadJobs())
    );
    this.jobData$.subscribe(data => {
      this.isLoading = false;
      this.listOfJob = data.filter(item => item.completed === this.completed);
      this.search();
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

}
