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

package nextflow.container

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class SingularityBuilder extends ContainerBuilder {

    final String image

    private String entryPoint

    private String runCommand


    SingularityBuilder(String name) {
        this.image = name
    }

    @Override
    SingularityBuilder params(Map params) {

        if( params.containsKey('entry') )
            this.entryPoint = params.entry

        return this
    }

    @Override
    SingularityBuilder build(StringBuilder result) {

        result << 'singularity exec '

        result << image

        runCommand = result.toString()

        return this
    }

    @Override
    String getRunCommand() {
        return runCommand
    }
}
