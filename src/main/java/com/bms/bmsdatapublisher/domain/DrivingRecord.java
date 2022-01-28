package com.bms.bmsdatapublisher.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.LocalDateTime;

@Data
@Entity
@NoArgsConstructor
public class DrivingRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    int id;

    private String carId;

    private LocalDateTime updatedTime;

    private Integer odometer;

    private Double socReal;

    private Double socDisplay;

    private Double vehicleSpeed;

    private Integer chargeStatus;

    private Integer keyStatus;

    private Double xcord;

    private Double ycord;

    public DrivingRecord(String carId, Integer odometer, Double socReal, Double socDisplay, Double vehicleSpeed, Integer chargeStatus, Integer keyStatus, Double xcord, Double yCord) {
        this(0,carId, odometer, socReal, socDisplay, vehicleSpeed, chargeStatus, keyStatus, xcord, yCord);
    }

    public DrivingRecord(int id, String carId, Integer odometer, Double socReal, Double socDisplay, Double vehicleSpeed, Integer chargeStatus, Integer keyStatus, Double xcord, Double ycord) {
        this.id = id;
        this.carId = carId;
        this.odometer = odometer;
        this.socReal = socReal;
        this.socDisplay = socDisplay;
        this.vehicleSpeed = vehicleSpeed;
        this.chargeStatus = chargeStatus;
        this.keyStatus = keyStatus;
        this.xcord = xcord;
        this.ycord = ycord;
    }


}