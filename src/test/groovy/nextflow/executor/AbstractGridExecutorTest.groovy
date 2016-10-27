/*
 * Copyright (c) 2013-2016, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2016, Paolo Di Tommaso and the respective authors.
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

package nextflow.executor

import nextflow.Session
import nextflow.processor.TaskRun
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class AbstractGridExecutorTest extends Specification {

    def 'should remove invalid chars from name' () {

        given:
        def task = new TaskRun(name: 'task 90 (foo:bar)')
        def exec = [:] as AbstractGridExecutor

        expect:
        exec.getJobNameFor(task) == 'nf-task_90_(foo_bar)'

    }

    def 'should return the kill list' () {

        given:
        def exec = [getKillCommand: { ['qdel'] }] as AbstractGridExecutor

        expect:
        exec.killTaskCommand('10') == ['qdel', '10']
        exec.killTaskCommand([11,12]) == ['qdel', '11', '12']
        exec.killTaskCommand([100,200,300]) == ['qdel', '100', '200', '300']

    }

    def 'should return a custom job name'() {

        given:
        def exec = [:] as AbstractGridExecutor
        exec.session = [:] as Session
        exec.session.config = [:]

        expect:
        exec.resolveCustomJobName(Mock(TaskRun)) == null

        when:
        exec.session = [:] as Session
        exec.session.config = [ executor: [jobName: { task.name.replace(' ','_') }  ] ]
        then:
        exec.resolveCustomJobName(new TaskRun(config: [name: 'hello world'])) == 'hello_world'

    }

    def 'should return job submit name' () {

        given:
        def exec = [:] as AbstractGridExecutor
        exec.session = [:] as Session
        exec.session.config = [:]

        final taskName = 'Hello world'
        final taskRun = new TaskRun(name: taskName, config: [name: taskName])

        expect:
        exec.getJobNameFor(taskRun) == 'nf-Hello_world'

        when:
        exec.session = [:] as Session
        exec.session.config = [ executor: [jobName: { task.name.replace(' ','_') }  ] ]
        then:
        exec.getJobNameFor(taskRun) == 'Hello_world'
    }

    def testPreemptExitStatus() {

        when:
        def exec1 = [:] as AbstractGridExecutor
        then:
        exec1.getPreemptExitStatus() == []
        !exec1.isPreemptExitStatus(0)
        !exec1.isPreemptExitStatus(1)

        when:
        def exec2 = [:] as AbstractGridExecutor
        exec2.session = new Session([executor: [preemptExitStatus: 100]])
        then:
        exec2.getPreemptExitStatus() == [100]
        !exec2.isPreemptExitStatus(0)
        exec2.isPreemptExitStatus(100)

        when:
        def exec3 = [:] as AbstractGridExecutor
        exec3.session = new Session([executor: [preemptExitStatus: [10,20,30]]])
        then:
        exec3.getPreemptExitStatus() == [10,20,30]
        !exec3.isPreemptExitStatus(0)
        exec3.isPreemptExitStatus(10)
        exec3.isPreemptExitStatus(20)
        exec3.isPreemptExitStatus(30)
    }
}
