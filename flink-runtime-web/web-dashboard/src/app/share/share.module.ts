import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { NgZorroAntdModule } from 'ng-zorro-antd';
import { PipeModule } from 'pipes/pipe.module';
import { DagreModule } from 'share/common/dagre/dagre.module';
import { GraphModule } from './common/graph';
import { PaginationComponent } from './common/pagination/pagination.component';
import { LayoutComponent } from './common/layout/layout.component';
import { TaskBadgeComponent } from './customize/task-badge/task-badge.component';
import { JobBadgeComponent } from './customize/job-badge/job-badge.component';
import { JobListComponent } from './customize/job-list/job-list.component';
import { MonacoEditorComponent } from './common/monaco-editor/monaco-editor.component';
import { NavigationComponent } from './common/navigation/navigation.component';
import { FileReadDirective } from './common/file-read/file-read.directive';
import { ResizeComponent } from './common/resize/resize.component';
import { JobChartComponent } from './customize/job-chart/job-chart.component';

@NgModule({
  imports     : [
    CommonModule,
    NgZorroAntdModule,
    PipeModule,
    RouterModule,
    FormsModule,
    DagreModule,
    GraphModule,
  ],
  declarations: [
    LayoutComponent,
    TaskBadgeComponent,
    JobBadgeComponent,
    JobListComponent,
    MonacoEditorComponent,
    NavigationComponent,
    FileReadDirective,
    ResizeComponent,
    JobChartComponent,
    PaginationComponent
  ],
  exports     : [
    LayoutComponent,
    TaskBadgeComponent,
    JobBadgeComponent,
    JobListComponent,
    DagreModule,
    GraphModule,
    MonacoEditorComponent,
    NavigationComponent,
    FileReadDirective,
    ResizeComponent,
    JobChartComponent,
    PaginationComponent
  ]
})
export class ShareModule {
}
