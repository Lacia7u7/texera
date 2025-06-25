/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Injectable } from "@angular/core";
import { Observable, Subject } from "rxjs";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import {TableProfile} from "../../../common/type/proto/edu/uci/ics/amber/engine/architecture/worker/tableprofile";

@Injectable({
  providedIn: "root",
})
export class WorkflowStatusService {
  // status is responsible for passing websocket responses to other components
  private statusSubject = new Subject<Record<string, OperatorStatistics>>();
  private currentStatus: Record<string, OperatorStatistics> = {};

  private tableProfileSubject = new Subject<Record<string, TableProfile>>();
  private currentTableProfile: Record<string, TableProfile> = {};

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    this.getStatusUpdateStream().subscribe(event => (this.currentStatus = event));
    this.getTableProfileUpdateStream().subscribe(event => {
      console.log(event); this.currentTableProfile = event;
    });

    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== "OperatorStatisticsUpdateEvent") {
        return;
      }
      this.statusSubject.next(event.operatorStatistics);
      this.tableProfileSubject.next(event.operatorResultTableProfile);
    });
  }

  public getTableProfileUpdateStream(): Observable<Record<string, TableProfile>> {
    return this.tableProfileSubject.asObservable();
  }

  public getStatusUpdateStream(): Observable<Record<string, OperatorStatistics>> {
    return this.statusSubject.asObservable();
  }

  public getCurrentStatus(): Record<string, OperatorStatistics> {
    return this.currentStatus;
  }

  public getCurrentTableProfile(): Record<string, TableProfile> {
    return this.currentTableProfile;
  }

  public resetStatus(): void {
    const initStatus: Record<string, OperatorStatistics> = Object.keys(this.currentStatus).reduce(
      (accumulator, operatorId) => {
        accumulator[operatorId] = {
          operatorState: OperatorState.Uninitialized,
          aggregatedInputRowCount: 0,
          aggregatedOutputRowCount: 0,
        };
        return accumulator;
      },
      {} as Record<string, OperatorStatistics>
    );
    this.statusSubject.next(initStatus);
    this.tableProfileSubject.next({});
  }

  public clearStatus(): void {
    this.currentStatus = {};
    this.statusSubject.next({});

    this.currentTableProfile = {};
    this.tableProfileSubject.next({});
  }
}
