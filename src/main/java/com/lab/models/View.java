package com.lab.models;

import java.io.Serializable;
import java.util.Date;

//import javax.persistence.Entity;
//import javax.persistence.GeneratedValue;
//import javax.persistence.Id;

/**
 * A Data Structure that represents a view of a video
 * 
 * @author taaabha1
 *
 */
//@Entity
public class View implements Serializable {
    
    private static final long serialVersionUID = 1947087677205353674L;
    
//    @GeneratedValue
//    @Id
    long viewId;
    long videoId;
    long userId;
    String location;
    String device;
    Date timeStamp;
    
    public View() {
        super();
    }
    
    public View(long videoId, long userId, String location, String device, Date timeStamp) {
        this.videoId = videoId;
        this.userId = userId;
        this.location = location;
        this.device = device;
        this.timeStamp = timeStamp;
    }
    
    public long getViewId() {
        return viewId;
    }

    public void setViewId(long viewId) {
        this.viewId = viewId;
    }

    public long getVideoId() {
        return videoId;
    }

    public void setVideoId(long videoId) {
        this.videoId = videoId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

}