import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NgZorroAntdModule } from 'ng-zorro-antd';
import { PipeModule } from '../../pipes/pipe.module';
import { ShareModule } from '../../share/share.module';

import { TaskManagerRoutingModule } from './task-manager-routing.module';
import { TaskManagerListComponent } from './list/task-manager-list.component';
import { TaskManagerMetricsComponent } from './metrics/task-manager-metrics.component';
import { TaskManagerDetailComponent } from './detail/task-manager-detail.component';
import { TaskManagerStatusComponent } from './status/task-manager-status.component';
import { TaskManagerLogsComponent } from './logs/task-manager-logs.component';
import { TaskManagerStdoutComponent } from './stdout/task-manager-stdout.component';

@NgModule({
  imports     : [
    CommonModule,
    NgZorroAntdModule,
    PipeModule,
    ShareModule,
    TaskManagerRoutingModule
  ],
  declarations: [
    TaskManagerListComponent,
    TaskManagerMetricsComponent,
    TaskManagerDetailComponent,
    TaskManagerStatusComponent,
    TaskManagerLogsComponent,
    TaskManagerStdoutComponent
  ]
})
export class TaskManagerModule {
}
