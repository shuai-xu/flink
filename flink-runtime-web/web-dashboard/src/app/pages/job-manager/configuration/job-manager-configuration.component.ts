import { Component, OnInit } from '@angular/core';
import { JobManagerService } from '../../../services/job-manager.service';

@Component({
  selector   : 'flink-job-manager-configuration',
  templateUrl: './job-manager-configuration.component.html',
  styleUrls  : [ './job-manager-configuration.component.less' ]
})
export class JobManagerConfigurationComponent implements OnInit {
  listOfConfig = [];

  constructor(private jobManagerService: JobManagerService) {
  }

  ngOnInit() {
    this.jobManagerService.loadConfig().subscribe(data => {
      this.listOfConfig = data;
    });
  }

}
