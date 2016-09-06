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
 * Wrap a task execution in a Udocker container
 *
 * See https://github.com/indigo-dc/udocker
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class UdockerBuilder implements ContainerBuilder {

    static private String UDOCKER_HELPERS = '''
        function udocker_run() {
            local image="$1"
            shift
            # pull the image if needed
            (udocker.py images | egrep -o "^$image\\s") || udocker.py pull "$image"
            # create the container
            id=$(udocker.py create "$image")
            udocker.py run -v $PWD -w $PWD $id "$@"
        }
        '''

    private String image

    UdockerBuilder( String image ) {
        this.image = image
    }


    @Override
    ContainerBuilder params(Map config) {
        return null
    }

    @Override
    String build(StringBuilder result) {
        assert image, 'Missing container image'

        for( def entry : env ) {
            result << makeEnv(entry) << ' '
        }

        result << 'shifter '


        result << '--image ' << image

        if( entryPoint )
            result << ' ' << entryPoint

        runCommand = result.toString()

    }

    @Override
    def getRunCommand() {
        return null
    }
}
