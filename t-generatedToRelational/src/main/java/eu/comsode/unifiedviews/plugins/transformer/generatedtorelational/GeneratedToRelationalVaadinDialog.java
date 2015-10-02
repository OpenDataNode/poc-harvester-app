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
