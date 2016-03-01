/**
 * Copyright 2015 Peter Goliuan.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package eu.comsode.unifiedviews.plugins.transformer.generatedtorelational;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.junit.Test;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;

import cz.cuni.mff.xrg.odcs.dpu.test.TestEnvironment;
import eu.comsode.unifiedviews.plugins.transformer.generatedtorelational.GeneratedToRelational;
import eu.comsode.unifiedviews.plugins.transformer.generatedtorelational.GeneratedToRelationalConfig_V1;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.helpers.dataunit.rdf.RDFHelper;
import eu.unifiedviews.helpers.dpu.test.config.ConfigurationBuilder;

public class GeneratedToRelationalTest {
    @Test
    public void test() throws Exception {
        GeneratedToRelationalConfig_V1 config = new GeneratedToRelationalConfig_V1();

        // Prepare DPU.
        GeneratedToRelational dpu = new GeneratedToRelational();
        dpu.configure((new ConfigurationBuilder()).setDpuConfiguration(config).toString());

        // Prepare test environment.
        TestEnvironment environment = new TestEnvironment();
        CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
        encoder.onMalformedInput(CodingErrorAction.REPORT);
        encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        PrintWriter outputWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("projects.txt", false), encoder)));

        // Prepare data unit.
        WritableRDFDataUnit rdfOutput = environment.createRdfOutput("rdfOutput", false);

        try {
            // Run.
            environment.run(dpu);

            RepositoryConnection con = rdfOutput.getConnection();
            con.export(Rio.createWriter(RDFFormat.TURTLE, outputWriter), RDFHelper.getGraphsURIArray(rdfOutput));
        } finally {
            // Release resources.
            environment.release();
        }
    }
}
