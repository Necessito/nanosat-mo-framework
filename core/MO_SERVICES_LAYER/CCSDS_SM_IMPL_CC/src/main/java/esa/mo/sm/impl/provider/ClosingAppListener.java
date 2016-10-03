/* ----------------------------------------------------------------------------
 * Copyright (C) 2015      European Space Agency
 *                         European Space Operations Centre
 *                         Darmstadt
 *                         Germany
 * ----------------------------------------------------------------------------
 * System                : ESA NanoSat MO Framework
 * ----------------------------------------------------------------------------
 * Licensed under the European Space Agency Public License, Version 2.0
 * You may not use this file except in compliance with the License.
 *
 * Except as expressly set forth in this License, the Software is provided to
 * You on an "as is" basis and without warranties of any kind, including without
 * limitation merchantability, fitness for a particular purpose, absence of
 * defects or errors, accuracy or non-infringement of intellectual property rights.
 * 
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 * ----------------------------------------------------------------------------
 */
package esa.mo.sm.impl.provider;

import esa.mo.com.impl.consumer.EventConsumerServiceImpl;
import esa.mo.com.impl.util.EventCOMObject;
import esa.mo.com.impl.util.EventReceivedListener;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ccsds.moims.mo.mal.MALException;
import org.ccsds.moims.mo.mal.MALInteractionException;
import org.ccsds.moims.mo.softwaremanagement.appslauncher.provider.StopAppInteraction;

/**
 * Create the listeners for the returned event by the app, acknowledging the
 * action to close itself.
 * @author Cesar Coelho
 */
public class ClosingAppListener extends EventReceivedListener {

    private final StopAppInteraction interaction;
    private final EventConsumerServiceImpl eventService;
    private final Long objId;

    public ClosingAppListener(final StopAppInteraction interaction, 
            final EventConsumerServiceImpl eventService, final Long objId) {
        this.interaction = interaction;
        this.eventService = eventService;
        this.objId = objId;
    }

    @Override
    public void onDataReceived(EventCOMObject eventCOMObject) {
        // Is it the ack from the app?
        if(true){ // To do: better comparison
            Logger.getLogger(ClosingAppListener.class.getName()).log(Level.INFO, "The app with objId " + objId + " is now closing...");
        }
        
        // If so, then close the connection to the service
        eventService.closeConnection();

        try { 
            // Send update to consumer stating that the app is closing...
            interaction.sendUpdate(objId);
        } catch (MALInteractionException ex) {
            Logger.getLogger(ClosingAppListener.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MALException ex) {
            Logger.getLogger(ClosingAppListener.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
