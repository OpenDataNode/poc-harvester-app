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

import eu.unifiedviews.dpu.config.DPUConfigException;
import eu.unifiedviews.helpers.dpu.vaadin.dialog.AbstractDialog;

/**
 * Vaadin configuration dialog .
 */
public class GeneratedToRelationalVaadinDialog extends AbstractDialog<GeneratedToRelationalConfig_V1> {

    private static final long serialVersionUID = 8715849977476398760L;

    public GeneratedToRelationalVaadinDialog() {
        super(GeneratedToRelational.class);
    }

    @Override
    public void setConfiguration(GeneratedToRelationalConfig_V1 c) throws DPUConfigException {

    }

    @Override
    public GeneratedToRelationalConfig_V1 getConfiguration() throws DPUConfigException {
        final GeneratedToRelationalConfig_V1 c = new GeneratedToRelationalConfig_V1();

        return c;
    }

    @Override
    public void buildDialogLayout() {
    }

}
