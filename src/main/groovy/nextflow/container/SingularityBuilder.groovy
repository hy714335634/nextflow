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

import java.nio.file.Path

import nextflow.util.Escape

/**
 * Implements a builder for Singularity containerisation
 *
 * see http://singularity.lbl.gov
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class SingularityBuilder extends ContainerBuilder {

    private String entryPoint

    private String runCommand

    SingularityBuilder(String name) {
        this.image = name
    }

    @Override
    SingularityBuilder params(Map params) {

        if( params.containsKey('entry') )
            this.entryPoint = params.entry

        if( params.containsKey('engineOptions') )
            addEngineOptions(params.engineOptions.toString())

        if( params.containsKey('runOptions') )
            addRunOptions(params.runOptions.toString())

        return this
    }

    SingularityBuilder addRunOptions(String str) {
        runOptions.add(str)
        return this
    }

    @Override
    SingularityBuilder build(StringBuilder result) {

        result << 'env - PATH=$PATH '

        result << 'singularity '

        if( engineOptions )
            result << engineOptions.join(' ') << ' '

        result << 'exec '

        if( runOptions )
            result << runOptions.join(' ') << ' '

        result << image

        if( entryPoint )
            result  << ' ' << entryPoint

        runCommand = result.toString()

        return this
    }

    @Override
    protected CharSequence makeEnv( env, StringBuilder result = new StringBuilder() ) {
        // append the environment configuration
        if( env instanceof File ) {
            env = env.toPath()
        }
        if( env instanceof Path ) {
            result << 'export BASH_ENV="' << Escape.path(env) << '"; '
        }
        else if( env instanceof Map ) {
            short index = 0
            for( Map.Entry entry : env.entrySet() ) {
                if( index++ ) result << ' '
                result << "export ${entry.key}=\"${entry.value}\"; "
            }
        }
        else if( env instanceof String && env.contains('=') ) {
            result << 'export ' << env << '; '
        }
        else if( env ) {
            throw new IllegalArgumentException("Not a valid environment value: $env [${env.class.name}]")
        }

        return result
    }

    String getEnvExports() {
        def result = new StringBuilder()
        for( def entry : env ) {
            makeEnv(entry, result)
        }
        return result.toString()
    }

    @Override
    String getRunCommand() {
        return runCommand
    }
}
