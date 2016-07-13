package org.apache.hadoop.mapreduce;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** An {@link OutputCommitter} that commits files specified 
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
 **/
public class DirectFileOutputCommitter extends OutputCommitter {
  private static final Log LOG = LogFactory.getLog(DirectFileOutputCommitter.class);

  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  public static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
      "mapreduce.fileoutputcommitter.marksuccessfuljobs";
  public static final String FILEOUTPUTCOMMITTER_ALGORITHM_VERSION =
      "mapreduce.fileoutputcommitter.algorithm.version";
  public static final int FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT = 1;
  private Path outputPath = null;

  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  public DirectFileOutputCommitter(Path outputPath, 
                             TaskAttemptContext context) throws IOException {
    this(outputPath, (JobContext)context);
  }
  
  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  public DirectFileOutputCommitter(Path outputPath, 
                             JobContext context) throws IOException {
    if (outputPath != null) {
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      this.outputPath = fs.makeQualified(outputPath);
    }
  }
  
  /**
   * @return the path where final output of the job should be placed.  This
   * could also be considered the committed application attempt path.
   */
  private Path getOutputPath() {
    return this.outputPath;
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @return the path to store job attempt data.
   */
  public Path getJobAttemptPath(JobContext context) {
    return getOutputPath();
  }
  
  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * 
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return getOutputPath();
  }
  
  /**
   * Get the directory that the task should write results into.
   * @return the work directory
   * @throws IOException
   */
  public Path getWorkPath() throws IOException {
    return getOutputPath();
  }

  public void setupJob(JobContext context) throws IOException {
      LOG.info("Empty job setup " + context.getJobID());
  }

  public void commitJob(JobContext context) throws IOException {
	  LOG.info("Commit job " + context.getJobID());
      Path finalOutput = getOutputPath();
      FileSystem fs = finalOutput.getFileSystem(context.getConfiguration());

      if (context.getConfiguration().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
    	LOG.info("Writting succes file " + context.getJobID());
        Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
        fs.create(markerPath).close();
      }
  }

  @Override
  @Deprecated
  public void cleanupJob(JobContext context) throws IOException {
	  LOG.info("Empty cleanup job " + context.getJobID());
  }

  /**
   * Delete the temporary directory, including all of the work directories.
   * @param context the job's context
   */
  @Override
  public void abortJob(JobContext context, JobStatus.State state) 
  throws IOException {
    // delete the _temporary folder
    cleanupJob(context);
        //also delete success marker when job is aborted
    Path finalOutput = getOutputPath();
    FileSystem fs = finalOutput.getFileSystem(context.getConfiguration());

    if (context.getConfiguration().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
    	LOG.info("Deleting succes file " + context.getJobID());
    	Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
    	fs.delete(markerPath, false);
    }
  }
  
  /**
   * No task setup required.
   */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
	  LOG.info("Empty setup task " + context.getTaskAttemptID());
  }

  /**
   * Move the files from the work directory to the job output directory
   * @param context the task context
   */
  @Override
  public void commitTask(TaskAttemptContext context) 
  throws IOException {
    commitTask(context, null);
  }

  @Private
  public void commitTask(TaskAttemptContext context, Path taskAttemptPath) 
      throws IOException {
	  LOG.info("Empty commit task " + context.getTaskAttemptID());
  }

  /**
   * Delete the work directory
   * @throws IOException 
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    abortTask(context, null);
  }

  @Private
  public void abortTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
	  LOG.info("Empty abort task " + context.getTaskAttemptID());
  }

  /**
   * Did this task write any files in the work directory?
   * @param context the task's context
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context
                                 ) throws IOException {
    return needsTaskCommit(context, null);
  }

  @Private
  public boolean needsTaskCommit(TaskAttemptContext context, Path taskAttemptPath
    ) throws IOException {
	  LOG.info("Need task commit - true " + context.getTaskAttemptID());
	  return true;
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return false;
  }
  
  @Override
  public void recoverTask(TaskAttemptContext context)
      throws IOException {
	  LOG.info("Empty Recover tasks " + context.getTaskAttemptID());
  }
}
