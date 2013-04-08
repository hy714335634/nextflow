/*
 * Copyright (c) 2012, the authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.processor

import groovy.io.FileType
import groovy.transform.InheritConstructors
import groovy.util.logging.Slf4j
import nextflow.util.ByteDumper
import org.apache.commons.io.IOUtils

/**
 * Execute a task script by running it on the SGE/OGE cluster
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@InheritConstructors
class OgeTaskProcessor extends AbstractTaskProcessor {

    private String queue

    private String qsubCmdLine

    OgeTaskProcessor queue( String queue0 ) {
        this.queue = queue0
        return this
    }

    OgeTaskProcessor qsubCmdLine( String cmdLine ) {
        this.qsubCmdLine = cmdLine
        return this
    }



    @Override
    protected void launchTask(TaskDef task) {

        task.workDirectory = shareWorkDir ? new File(session.workDirectory, name) : new File(session.workDirectory, "$name-${task.index}")
        File scratch = task.workDirectory

        if ( !scratch.exists() && !scratch.mkdirs() ) {
            throw new IOException("Unable to create task work directory: '${scratch}'")
        }

        log.debug "Lauching task > ${task.name} -- scratch folder: $scratch"

        /*
         * save the original script to be executed
         */
        def scriptFile = new File(scratch, '.command.sh')
        scriptFile.text = task.script.toString()

        // -- keep a reference to the file
        task.script = scriptFile

        /*
         * Prepare the 'qsub' cmdline. The following options are used
         * - wd: define the job working directory
         * - terse: output just the job id on the output stream
         * - o: define the file to which redirect the standard output
         * - e: define the file to which redirect the error output
         */

        File cmdOutFile = new File(scratch, '.command.out').absoluteFile
        def qsubCmd = "qsub -terse -wd \$PWD -o ${cmdOutFile} -j y -sync y -V "

        // add other parameters (if any)
        if(queue) {
            qsubCmd += "-q ${queue} "
        }

//        if( config?.walltime )  {
//            startCmd += "-l h_rt=${config.walltime} "
//        }
//
//        if( config?.procs && config.procs.toString().isInteger() ) {
//            startCmd +=  "-l slots=${config.procs} "
//        }
//        else if ( config?.procs ) {
//            startCmd += "-pe ${config.procs} "
//        }
//
//        if( config?.memory ) {
//            /*
//             * Read more about SGE virtual_free vs mem_free at the following links
//             * http://gridengine.org/pipermail/users/2011-December/002215.html
//             * http://www.gridengine.info/tag/virtual_free/
//             */
//            startCmd += "-l virtual_free=${config.memory} "
//        }
//
//        if( config?.sge_request_options ) {
//            startCmd += config.sge_request_options + ' '
//        }

        // -- the job name
        qsubCmd += "-N nf-${name}-${task.index} "

        // at the end append the command script wrapped file name
        if ( qsubCmdLine ) {
            qsubCmd += "$qsubCmdLine "
        }

        // append at the the script file to be executed through 'qsub'
        qsubCmd += scriptFile

        // -- log the qsub command
        log.debug "qsub command > ${qsubCmd} -- task: ${task.name}"

        /*
         * Save the 'qsub' command line
         */
        def qsubLauncher = new File(scratch, '.qsub.sh')
        qsubLauncher.text = qsubCmd


        /*
         * launch 'qsub' script wrapper
         */
        ProcessBuilder builder = new ProcessBuilder()
                .directory(scratch)
                .command( shell, qsubLauncher.toString() )
                .redirectErrorStream(true)

        // -- configure the job environment
        builder.environment().putAll(getProcessEnvironment())

        // -- start the execution and notify the event to the monitor
        Process process = builder.start()
        task.status = TaskDef.Status.RUNNING

        // -- pipe the input value to the process standard input
        if( task.input != null ) {
            try {
                process.withOutputStream{ writer -> writer << task.input }
            }
            catch( IOException e ) {
                log.warn "Unable to pipe input data for task: ${task.name}", e
            }
        }

        // -- save the 'qsub' process output
        def qsubOutFile = new File(scratch, '.qsub.out')
        def qsubOutStream = new BufferedOutputStream(new FileOutputStream(qsubOutFile))
        ByteDumper qsubDumper = new ByteDumper(process.getInputStream(), {  byte[] data, int len -> qsubOutStream.write(data,0,len) } )
        qsubDumper.setName("qsub-$name")
        qsubDumper.start()

        // -- print the process out if it is not capture by the output
        //    * The byte dumper uses a separate thread to capture the process stdout
        //    * The process stdout is captured in two condition:
        //      when the flag 'echo' is set or when it goes in the output channel (outputs['-'])
        //
        def handler = echo ? { byte[] data, int len ->  System.out.print(new String(data,0,len)) } : null
        ByteDumper cmdDumper = new ByteDumper( cmdOutFile, handler )
        cmdDumper.setName("dumper-$name")
        cmdDumper.start()

        def success = false
        try {
            // -- wait the the process completes
            task.exitCode = process.waitFor()
            success = task.exitCode in validExitCodes
            log.debug "Task completeted > ${task.name} -- exit code: ${task.exitCode}; accepted code(s): ${validExitCodes.join(',')}"

            qsubDumper.await(500)
            qsubOutStream.close()

            // there may be very loooong delay over NFS, wait at least one minute
            cmdDumper.await(60_000)

        }
        finally {
            qsubDumper.terminate()
            cmdDumper.terminate()
            task.workDirectory = scratch

            IOUtils.closeQuietly(process.in)
            IOUtils.closeQuietly(process.out)
            IOUtils.closeQuietly(process.err)
            process.destroy()

            //  -- return the program output with the following strategy
            //   + program terminated ok -> return the program output output (file)
            //   + program failed and output file not empty -> program output
            //             failed and output EMPTY -> return 'qsub' output file
            log.debug "Task cmd output > ${task.name} -- file ${cmdOutFile}; empty: ${cmdOutFile.isEmpty()}"
            log.debug "Task qsub output > ${task.name} -- file: ${qsubOutFile}; empty: ${qsubOutFile.isEmpty()}"

            if( success ) {
                task.output = cmdOutFile.isNotEmpty() ? cmdOutFile : ''
            }
            else {
                task.output = cmdOutFile.isNotEmpty() ? cmdOutFile : ( qsubOutFile.isNotEmpty() ? qsubOutFile : '' )
            }

            log.debug "Task finished > ${task.name} -- success: ${success}; output: ${task.@output}"

        }

    }




    @Override
    protected List<File> collectResultFile( File scratchPath, String name ) {
        assert scratchPath
        assert name

        // replace any wildcards characters
        // TODO give a try to http://code.google.com/p/wildcard/  -or- http://commons.apache.org/io/
        String filePattern = name.replace("?", ".?").replace("*", ".*?")

        if( filePattern == name ) {
            // TODO check that the file exists (?)
            return [ new File(scratchPath,name) ]
        }

        // scan to find the file with that name
        List files = []
        scratchPath.eachFileMatch(FileType.FILES, ~/$filePattern/ ) { File it -> files << it}

        // TODO ++ what if expected files are missing?
        return files
    }




}
