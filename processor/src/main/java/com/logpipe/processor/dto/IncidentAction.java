package com.logpipe.processor.dto;

public enum IncidentAction {
    OPEN,           // open a new incident (FIRING)
    START_BELOW,    // the service just dropped below the threshold: remember when
    RESET_BELOW,    // the service went back above the threshold: forget belowSince
    RESOLVE,        // below the threshold long enough: close the incident (RESOLVED)
    NONE            // nothing to do
}
