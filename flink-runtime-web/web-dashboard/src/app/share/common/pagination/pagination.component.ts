import { Component, EventEmitter, OnInit, Output } from '@angular/core';

@Component({
  selector   : 'flink-pagination',
  templateUrl: './pagination.component.html',
  styleUrls  : [ './pagination.component.less' ]
})
export class PaginationComponent implements OnInit {
  @Output() pageChanged = new EventEmitter();
  page = -1;

  goToPage(page) {
    this.page = page;
    this.pageChanged.emit(page);
  }

  constructor() {
  }

  ngOnInit() {
  }

}
