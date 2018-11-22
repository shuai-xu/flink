import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { TaskManagerComponent } from './task-manager.component';
import { TaskManagerListComponent } from './list/task-manager-list.component';
import { TaskManagerMetricsComponent } from './metrics/task-manager-metrics.component';
import { TaskManagerLogDetailComponent } from './log-detail/task-manager-log-detail.component';
import { TaskManagerLogListComponent } from './log-list/task-manager-log-list.component';

const routes: Routes = [
  {
    path     : '',
    component: TaskManagerListComponent
  },
  {
    path     : ':taskManagerId',
    component: TaskManagerComponent,
    children : [
      {
        path     : 'metrics',
        component: TaskManagerMetricsComponent,
        data     : {
          path: 'metrics'
        }
      },
      {
        path     : 'log',
        component: TaskManagerLogListComponent,
        data     : {
          path: 'log'
        }
      },
      {
        path     : 'log/:logName',
        component: TaskManagerLogDetailComponent,
        data     : {
          path: 'log'
        }
      },
      {
        path      : '**',
        redirectTo: 'metrics',
        pathMatch : 'full'
      }
    ]
  }
];

@NgModule({
  imports: [ RouterModule.forChild(routes) ],
  exports: [ RouterModule ]
})
export class TaskManagerRoutingModule {
}
