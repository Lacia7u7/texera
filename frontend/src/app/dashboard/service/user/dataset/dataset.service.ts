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
import { HttpClient, HttpParams } from "@angular/common/http";
import { map, switchMap } from "rxjs/operators";
import { Dataset, DatasetVersion } from "../../../../common/type/dataset";
import { AppSettings } from "../../../../common/app-setting";
import { Observable } from "rxjs";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { AuthService } from "src/app/common/service/user/auth.service";
import { getWebsocketUrl } from "../../../../common/util/url";

export const DATASET_BASE_URL = "dataset";
export const DATASET_CREATE_URL = DATASET_BASE_URL + "/create";
export const DATASET_UPDATE_BASE_URL = DATASET_BASE_URL + "/update";
export const DATASET_UPDATE_NAME_URL = DATASET_UPDATE_BASE_URL + "/name";
export const DATASET_UPDATE_DESCRIPTION_URL = DATASET_UPDATE_BASE_URL + "/description";
export const DATASET_UPDATE_PUBLICITY_URL = "update/publicity";
export const DATASET_UPDATE_DOWNLOADABLE_URL = "update/downloadable";
export const DATASET_LIST_URL = DATASET_BASE_URL + "/list";
export const DATASET_SEARCH_URL = DATASET_BASE_URL + "/search";
export const DATASET_DELETE_URL = DATASET_BASE_URL + "/delete";

export const DATASET_VERSION_BASE_URL = "version";
export const DATASET_VERSION_RETRIEVE_LIST_URL = DATASET_VERSION_BASE_URL + "/list";
export const DATASET_VERSION_LATEST_URL = DATASET_VERSION_BASE_URL + "/latest";
export const DEFAULT_DATASET_NAME = "Untitled dataset";
export const DATASET_PUBLIC_VERSION_BASE_URL = "publicVersion";
export const DATASET_PUBLIC_VERSION_RETRIEVE_LIST_URL = DATASET_PUBLIC_VERSION_BASE_URL + "/list";
export const DATASET_GET_OWNERS_URL = DATASET_BASE_URL + "/user-dataset-owners";

export interface MultipartUploadProgress {
  filePath: string;
  percentage: number;
  status: "initializing" | "uploading" | "finished" | "aborted";
  uploadSpeed?: number; // bytes per second
  estimatedTimeRemaining?: number; // seconds
  totalTime?: number; // total seconds taken
}

@Injectable({
  providedIn: "root",
})
export class DatasetService {
  constructor(
    private http: HttpClient,
    private config: GuiConfigService
  ) {}

  public createDataset(dataset: Dataset): Observable<DashboardDataset> {
    return this.http.post<DashboardDataset>(`${AppSettings.getApiEndpoint()}/${DATASET_CREATE_URL}`, {
      datasetName: dataset.name,
      datasetDescription: dataset.description,
      isDatasetPublic: dataset.isPublic,
      isDatasetDownloadable: dataset.isDownloadable,
    });
  }

  public getDataset(did: number, isLogin: boolean = true): Observable<DashboardDataset> {
    const apiUrl = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/public/${did}`;
    return this.http.get<DashboardDataset>(apiUrl);
  }

  /**
   * Retrieves a single file from a dataset version using a pre-signed URL.
   * @param filePath Relative file path within the dataset.
   * @param isLogin Determine whether a user is currently logged in
   * @returns Observable<Blob>
   */
  public retrieveDatasetVersionSingleFile(filePath: string, isLogin: boolean = true): Observable<Blob> {
    const endpointSegment = isLogin ? "presign-download" : "public-presign-download";
    const endpoint = `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${endpointSegment}?filePath=${encodeURIComponent(filePath)}`;

    return this.http
      .get<{ presignedUrl: string }>(endpoint)
      .pipe(switchMap(({ presignedUrl }) => this.http.get(presignedUrl, { responseType: "blob" })));
  }

  /**
   * Retrieves a zip file of a dataset version.
   * @param did Dataset ID
   * @param dvid (Optional) Dataset version ID. If omitted, the latest version is downloaded.
   * @returns An Observable that emits a Blob containing the zip file.
   */
  public retrieveDatasetVersionZip(did: number, dvid?: number): Observable<Blob> {
    let params = new HttpParams();

    if (dvid !== undefined && dvid !== null) {
      params = params.set("dvid", dvid.toString());
    } else {
      params = params.set("latest", "true");
    }

    return this.http.get(`${AppSettings.getApiEndpoint()}/dataset/${did}/versionZip`, {
      params,
      responseType: "blob",
    });
  }

  public retrieveAccessibleDatasets(): Observable<DashboardDataset[]> {
    return this.http.get<DashboardDataset[]>(`${AppSettings.getApiEndpoint()}/${DATASET_LIST_URL}`);
  }

  public createDatasetVersion(did: number, newVersion: string): Observable<DatasetVersion> {
    return this.http
      .post<{
        datasetVersion: DatasetVersion;
        fileNodes: DatasetFileNode[];
      }>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/version/create`, newVersion, {
        headers: { "Content-Type": "text/plain" },
      })
      .pipe(
        map(response => {
          response.datasetVersion.fileNodes = response.fileNodes;
          return response.datasetVersion;
        })
      );
  }

  /**
   * Handles multipart upload for large files using RxJS,
   * with a concurrency limit on how many parts we process in parallel.
   *
   * Backend flow:
   *   POST /dataset/multipart-upload?type=init&ownerEmail=...&datasetName=...&filePath=...&numParts=N
   *   POST /dataset/multipart-upload/part?ownerEmail=...&datasetName=...&filePath=...&partNumber=<n>  (body: raw chunk)
   *   POST /dataset/multipart-upload?type=finish&ownerEmail=...&datasetName=...&filePath=...
   *   POST /dataset/multipart-upload?type=abort&ownerEmail=...&datasetName=...&filePath=...
   */
  public multipartUpload(
    ownerEmail: string,
    datasetName: string,
    filePath: string,
    file: File,
    partSize: number,
    concurrencyLimit: number
  ): Observable<MultipartUploadProgress> {
    return new Observable<MultipartUploadProgress>(observer => {
      const partProgress = new Map<number, number>();
      const xhrs = new Map<number, XMLHttpRequest>();

      let startTime: number | null = null;
      const speedSamples: number[] = [];
      let lastETA = 0;
      let lastUpdateTime = 0;
      let inFlight = 0;
      let concurrency = concurrencyLimit;
      let completedParts = 0;
      let finishRequested = false;
      let closed = false;
      let noPartsTimer: number | null = null;
      const totalParts = Math.max(1, Math.ceil(file.size / partSize));

      const lastStats = {
        uploadSpeed: 0,
        estimatedTimeRemaining: 0,
        totalTime: 0,
      };

      const getTotalTime = () => (startTime ? (Date.now() - startTime) / 1000 : 0);

      const getLocalUploadedBytes = () => {
        let totalUploaded = 0;
        partProgress.forEach(bytes => {
          totalUploaded += bytes;
        });
        return totalUploaded;
      };

      const getServerCompletedBytes = () => {
        const normalizedCompleted = Math.min(completedParts, totalParts);
        if (normalizedCompleted <= 0) {
          return 0;
        }
        if (normalizedCompleted >= totalParts) {
          return file.size;
        }
        return normalizedCompleted * partSize;
      };

      const calculateStats = (totalUploaded: number) => {
        if (startTime === null) {
          if (totalUploaded <= 0) {
            return lastStats;
          }
          startTime = Date.now();
        }

        const now = Date.now();
        const elapsed = getTotalTime();

        const shouldUpdate = now - lastUpdateTime >= 1000;
        if (!shouldUpdate) {
          lastStats.totalTime = elapsed;
          return lastStats;
        }
        lastUpdateTime = now;

        const currentSpeed = elapsed > 0 ? totalUploaded / elapsed : 0;
        speedSamples.push(currentSpeed);
        if (speedSamples.length > 5) {
          speedSamples.shift();
        }
        const avgSpeed = speedSamples.length > 0 ? speedSamples.reduce((a, b) => a + b, 0) / speedSamples.length : 0;

        const remaining = file.size - totalUploaded;
        let eta = avgSpeed > 0 ? remaining / avgSpeed : 0;
        eta = Math.min(eta, 24 * 60 * 60);

        if (lastETA > 0 && eta > 0) {
          const maxChange = lastETA * 0.3;
          const diff = Math.abs(eta - lastETA);
          if (diff > maxChange) {
            eta = lastETA + (eta > lastETA ? maxChange : -maxChange);
          }
        }
        lastETA = eta;

        const percentComplete = (totalUploaded / file.size) * 100;
        if (percentComplete > 95) {
          eta = Math.min(eta, 10);
        }

        lastStats.uploadSpeed = avgSpeed;
        lastStats.estimatedTimeRemaining = Math.max(0, Math.round(eta));
        lastStats.totalTime = elapsed;

        return lastStats;
      };

      const websocketBaseUrl = getWebsocketUrl("wsapi/dataset-upload", "");
      const websocket = new WebSocket(websocketBaseUrl);
      const accessToken = AuthService.getAccessToken();

      const sendMessage = (type: string, data: Record<string, unknown>) => {
        const payload: Record<string, unknown> = {
          type,
          v: 1,
          ts: Date.now(),
          data,
        };
        if (websocket.readyState === WebSocket.OPEN) {
          websocket.send(JSON.stringify(payload));
        }
      };

      const emitProgress = (status: MultipartUploadProgress["status"]) => {
        const totalUploaded = getServerCompletedBytes() + getLocalUploadedBytes();
        const stats = calculateStats(totalUploaded);
        const percentage = file.size > 0 ? Math.min(100, (totalUploaded / file.size) * 100) : 0;
        observer.next({
          filePath,
          percentage,
          status,
          ...stats,
        });
      };

      const requestParts = () => {
        if (closed) return;
        const freeSlots = concurrency - inFlight;
        if (freeSlots > 0) {
          sendMessage("request_parts", { limit: freeSlots, ownerEmail, datasetName, filePath });
        }
      };

      const finishUpload = () => {
        if (finishRequested) return;
        finishRequested = true;
        const finishParams = new HttpParams()
          .set("type", "finish")
          .set("ownerEmail", ownerEmail)
          .set("datasetName", datasetName)
          .set("filePath", encodeURIComponent(filePath));
        this.http
          .post(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload`, {}, { params: finishParams })
          .subscribe({
            error: err => handleError(err as Error),
          });
      };

      const handleError = (error: Error) => {
        if (closed) return;
        emitProgress("aborted");
        if (!closed) {
          sendMessage("abort", { reason: "error", ownerEmail, datasetName, filePath });
        }
        observer.error(error);
        cleanup(false);
      };

      const startPartUpload = (partNumber: number) => {
        const index = partNumber - 1;
        const start = index * partSize;
        const end = Math.min(start + partSize, file.size);
        const chunk = file.slice(start, end);

        inFlight += 1;
        partProgress.set(partNumber, 0);

        const xhr = new XMLHttpRequest();
        xhrs.set(partNumber, xhr);

        xhr.upload.addEventListener("progress", event => {
          if (event.lengthComputable) {
            partProgress.set(partNumber, event.loaded);
            emitProgress("uploading");
          }
        });

        xhr.addEventListener("load", () => {
          inFlight -= 1;
          xhrs.delete(partNumber);

          if (xhr.status === 200 || xhr.status === 204) {
            partProgress.delete(partNumber);
            lastUpdateTime = 0;
            emitProgress("uploading");
            requestParts();
            return;
          }

          if (xhr.status === 409) {
            partProgress.delete(partNumber);
            requestParts();
            return;
          }

          partProgress.delete(partNumber);
          handleError(new Error(`Failed to upload part ${partNumber} (HTTP ${xhr.status})`));
        });

        xhr.addEventListener("error", () => {
          inFlight -= 1;
          xhrs.delete(partNumber);
          partProgress.delete(partNumber);
          handleError(new Error(`Failed to upload part ${partNumber}`));
        });

        const partUrl =
          `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload/part` +
          `?ownerEmail=${encodeURIComponent(ownerEmail)}` +
          `&datasetName=${encodeURIComponent(datasetName)}` +
          `&filePath=${encodeURIComponent(filePath)}` +
          `&partNumber=${partNumber}`;

        xhr.open("POST", partUrl);
        xhr.setRequestHeader("Content-Type", "application/octet-stream");
        const authToken = AuthService.getAccessToken();
        if (authToken) {
          xhr.setRequestHeader("Authorization", `Bearer ${authToken}`);
        }
        xhr.send(chunk);
      };

      const cleanup = (sendAbort: boolean) => {
        if (closed) return;
        closed = true;
        if (noPartsTimer !== null) {
          window.clearTimeout(noPartsTimer);
        }
        xhrs.forEach(xhr => {
          try {
            xhr.abort();
          } catch {}
        });
        if (sendAbort && websocket.readyState === WebSocket.OPEN) {
          sendMessage("abort", { reason: "client", ownerEmail, datasetName, filePath });
        }
        if (websocket.readyState === WebSocket.OPEN || websocket.readyState === WebSocket.CONNECTING) {
          websocket.close();
        }
      };

      websocket.onopen = () => {
        sendMessage("init", {
          ownerEmail,
          datasetName,
          filePath,
          fileSizeBytes: file.size,
          partSizeBytes: partSize,
          accessToken: accessToken ?? undefined,
        });
      };

      websocket.onmessage = event => {
        const message = JSON.parse(event.data);
        switch (message.type) {
          case "init_ack": {
            completedParts = message.data?.completedParts ?? 0;
            concurrency = concurrencyLimit;
            observer.next({
              filePath,
              percentage: file.size > 0 ? Math.min(100, (getServerCompletedBytes() / file.size) * 100) : 0,
              status: "initializing",
              uploadSpeed: 0,
              estimatedTimeRemaining: 0,
              totalTime: 0,
            });
            requestParts();
            break;
          }
          case "parts": {
            const parts = message.data?.parts ?? [];
            completedParts = message.data?.completedParts ?? completedParts;
            parts.forEach((part: { partNumber: number }) => {
              startPartUpload(part.partNumber);
            });
            break;
          }
          case "no_parts": {
            const retryAfterMs = message.data?.retryAfterMs ?? 500;
            if (noPartsTimer !== null) {
              window.clearTimeout(noPartsTimer);
            }
            noPartsTimer = window.setTimeout(() => requestParts(), retryAfterMs);
            break;
          }
          case "ping": {
            sendMessage("pong", {});
            break;
          }
          case "goodbye": {
            if (message.data?.reason === "finished") {
              observer.next({
                filePath,
                percentage: 100,
                status: "finished",
                uploadSpeed: 0,
                estimatedTimeRemaining: 0,
                totalTime: getTotalTime(),
              });
              observer.complete();
            } else {
              observer.next({
                filePath,
                percentage: file.size > 0 ? Math.min(100, (getServerCompletedBytes() / file.size) * 100) : 0,
                status: "aborted",
                uploadSpeed: 0,
                estimatedTimeRemaining: 0,
                totalTime: getTotalTime(),
              });
              observer.complete();
            }
            cleanup(false);
            break;
          }
          case "error": {
            handleError(new Error(message.data?.message ?? "Upload websocket error"));
            break;
          }
        }
      };

      websocket.onerror = () => {
        handleError(new Error("Upload websocket failed"));
      };

      websocket.onclose = () => {
        if (!closed) {
          handleError(new Error("Upload websocket closed unexpectedly"));
        }
      };

      return () => cleanup(true);
    });
  }

  public finalizeMultipartUpload(
    ownerEmail: string,
    datasetName: string,
    filePath: string,
    isAbort: boolean
  ): Observable<Response> {
    const params = new HttpParams()
      .set("type", isAbort ? "abort" : "finish")
      .set("ownerEmail", ownerEmail)
      .set("datasetName", datasetName)
      .set("filePath", encodeURIComponent(filePath));

    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload`,
      {},
      { params }
    );
  }

  /**
   * Resets a dataset file difference in LakeFS.
   * @param did Dataset ID
   * @param filePath File path to reset
   */
  public resetDatasetFileDiff(did: number, filePath: string): Observable<Response> {
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));

    return this.http.put<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/diff`, {}, { params });
  }

  /**
   * Deletes a dataset file from LakeFS.
   * @param did Dataset ID
   * @param filePath File path to delete
   */
  public deleteDatasetFile(did: number, filePath: string): Observable<Response> {
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));

    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/file`, { params });
  }

  /**
   * Retrieves the list of uncommitted dataset changes (diffs).
   * @param did Dataset ID
   */
  public getDatasetDiff(did: number): Observable<DatasetStagedObject[]> {
    return this.http.get<DatasetStagedObject[]>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/diff`);
  }

  /**
   * retrieve a list of versions of a dataset. The list is sorted so that the latest versions are at front.
   * @param did
   * @param isLogin
   */
  public retrieveDatasetVersionList(did: number, isLogin: boolean = true): Observable<DatasetVersion[]> {
    const apiEndPont = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_RETRIEVE_LIST_URL}`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_PUBLIC_VERSION_RETRIEVE_LIST_URL}`;
    return this.http.get<DatasetVersion[]>(apiEndPont);
  }

  /**
   * retrieve the latest version of a dataset.
   * @param did
   */
  public retrieveDatasetLatestVersion(did: number): Observable<DatasetVersion> {
    return this.http
      .get<{
        datasetVersion: DatasetVersion;
        fileNodes: DatasetFileNode[];
      }>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_LATEST_URL}`)
      .pipe(
        map(response => {
          response.datasetVersion.fileNodes = response.fileNodes;
          return response.datasetVersion;
        })
      );
  }

  /**
   * retrieve a list of nodes that represent the files in the version
   * @param did
   * @param dvid
   * @param isLogin
   */
  public retrieveDatasetVersionFileTree(
    did: number,
    dvid: number,
    isLogin: boolean = true
  ): Observable<{ fileNodes: DatasetFileNode[]; size: number }> {
    const apiUrl = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_BASE_URL}/${dvid}/rootFileNodes`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_PUBLIC_VERSION_BASE_URL}/${dvid}/rootFileNodes`;
    return this.http.get<{ fileNodes: DatasetFileNode[]; size: number }>(apiUrl);
  }

  public deleteDatasets(did: number): Observable<Response> {
    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}`);
  }

  public updateDatasetName(did: number, name: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_UPDATE_NAME_URL}`, {
      did: did,
      name: name,
    });
  }

  public updateDatasetDescription(did: number, description: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_UPDATE_DESCRIPTION_URL}`, {
      did: did,
      description: description,
    });
  }

  public updateDatasetPublicity(did: number): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_UPDATE_PUBLICITY_URL}`,
      {}
    );
  }

  public updateDatasetDownloadable(did: number): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_UPDATE_DOWNLOADABLE_URL}`,
      {}
    );
  }

  public retrieveOwners(): Observable<string[]> {
    return this.http.get<string[]>(`${AppSettings.getApiEndpoint()}/${DATASET_GET_OWNERS_URL}`);
  }

  public updateDatasetCoverImage(did: number, coverImage: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/dataset/${did}/update/cover`, {
      coverImage: coverImage,
    });
  }
}
