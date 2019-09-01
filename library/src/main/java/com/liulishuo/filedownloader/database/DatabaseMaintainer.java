/*
 * Copyright (c) 2015 LingoChamp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liulishuo.filedownloader.database;

import com.liulishuo.filedownloader.model.FileDownloadModel;
import com.liulishuo.filedownloader.model.FileDownloadStatus;
import com.liulishuo.filedownloader.util.FileDownloadExecutors;
import com.liulishuo.filedownloader.util.FileDownloadHelper;
import com.liulishuo.filedownloader.util.FileDownloadLog;
import com.liulishuo.filedownloader.util.FileDownloadUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseMaintainer {

    private final ThreadPoolExecutor maintainThreadPool;
    private final FileDownloadDatabase.Maintainer maintainer;
    private final FileDownloadHelper.IdGenerator idGenerator;

    public DatabaseMaintainer(FileDownloadDatabase.Maintainer maintainer,
                              FileDownloadHelper.IdGenerator idGenerator) {
        this.maintainer = maintainer;
        this.idGenerator = idGenerator;
        this.maintainThreadPool = FileDownloadExecutors.newDefaultThreadPool(3,
                FileDownloadUtils.getThreadPoolName("MaintainDatabase"));
    }

    public void doMaintainAction() {
        final Iterator<FileDownloadModel> iterator = maintainer.iterator();

        final AtomicInteger removedDataCount = new AtomicInteger(0);
        final AtomicInteger resetIdCount = new AtomicInteger(0);
        final AtomicInteger refreshDataCount = new AtomicInteger(0);

        final long startTimestamp = System.currentTimeMillis();
        final List<Future> futures = new ArrayList<>();
        try {
            while (iterator.hasNext()) {
                final FileDownloadModel model = iterator.next();
                final Future modelFuture = maintainThreadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        boolean isInvalid = false;
                        do {
                            if (model.getStatus() == FileDownloadStatus.progress
                                    || model.getStatus() == FileDownloadStatus.connected
                                    || model.getStatus() == FileDownloadStatus.error
                                    || (model.getStatus() == FileDownloadStatus.pending && model
                                    .getSoFar() > 0)
                            ) {
                                // Ensure can be covered by RESUME FROM BREAKPOINT.
                                model.setStatus(FileDownloadStatus.paused);
                            }
                            final String targetFilePath = model.getTargetFilePath();
                            if (targetFilePath == null) {
                                // no target file path, can't used to resume from breakpoint.
                                isInvalid = true;
                                break;
                            }

                            final File targetFile = new File(targetFilePath);
                            if (model.getStatus() == FileDownloadStatus.paused
                                    && FileDownloadUtils.isBreakpointAvailable(model.getId(), model,
                                    model.getPath(), null)) {
                                // can be reused in the old mechanism(no-temp-file).

                                final File tempFile = new File(model.getTempFilePath());

                                if (!tempFile.exists() && targetFile.exists()) {
                                    final boolean successRename = targetFile.renameTo(tempFile);
                                    if (FileDownloadLog.NEED_LOG) {
                                        FileDownloadLog.d(DatabaseMaintainer.class,
                                                "resume from the old no-temp-file architecture "
                                                        + "[%B], [%s]->[%s]",
                                                successRename, targetFile.getPath(),
                                                tempFile.getPath());

                                    }
                                }
                            }

                            /**
                             * Remove {@code model} from DB if it can't used for judging whether the
                             * old-downloaded file is valid for reused & it can't used for
                             * resuming from BREAKPOINT, In other words, {@code model} is no
                             * use anymore for FileDownloader.
                             */
                            if (model.getStatus() == FileDownloadStatus.pending
                                    && model.getSoFar() <= 0) {
                                // This model is redundant.
                                isInvalid = true;
                                break;
                            }

                            if (!FileDownloadUtils.isBreakpointAvailable(model.getId(), model)) {
                                // It can't used to resuming from breakpoint.
                                isInvalid = true;
                                break;
                            }

                            if (targetFile.exists()) {
                                // It has already completed downloading.
                                isInvalid = true;
                                break;
                            }

                        } while (false);

                        if (isInvalid) {
                            maintainer.onRemovedInvalidData(model);
                            removedDataCount.addAndGet(1);
                        } else {
                            final int oldId = model.getId();
                            final int newId = idGenerator.transOldId(oldId,
                                    model.getUrl(), model.getPath(),
                                    model.isPathAsDirectory());
                            if (newId != oldId) {
                                if (FileDownloadLog.NEED_LOG) {
                                    FileDownloadLog.d(DatabaseMaintainer.class,
                                            "the id is changed on restoring from db:"
                                                    + " old[%d] -> new[%d]",
                                            oldId, newId);
                                }
                                model.setId(newId);
                                maintainer.changeFileDownloadModelId(oldId, model);
                                resetIdCount.addAndGet(1);
                            }

                            maintainer.onRefreshedValidData(model);
                            refreshDataCount.addAndGet(1);
                        }
                    }
                });
                futures.add(modelFuture);
            }
        } finally {
            final Future markConvertedFuture = maintainThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    FileDownloadUtils.markConverted(FileDownloadHelper.getAppContext());
                }
            });
            futures.add(markConvertedFuture);
            final Future finishMaintainFuture = maintainThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    maintainer.onFinishMaintain();
                }
            });
            futures.add(finishMaintainFuture);
            for (Future future : futures) {
                try {
                    if (!future.isDone()) future.get();
                } catch (Exception e) {
                    if (FileDownloadLog.NEED_LOG) FileDownloadLog.e(this, e, e.getMessage());
                }
            }
            futures.clear();
            if (FileDownloadLog.NEED_LOG) {
                FileDownloadLog.d(this,
                        "refreshed data count: %d , delete data count: %d, reset id count:"
                                + " %d. consume %d",
                        refreshDataCount.get(), removedDataCount.get(), resetIdCount.get(),
                        System.currentTimeMillis() - startTimestamp);
            }
        }
    }
}