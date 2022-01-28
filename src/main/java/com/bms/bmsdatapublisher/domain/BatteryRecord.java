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
public class BatteryRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    private String carId;

    private LocalDateTime colecTime;

    private Double stateOfChrgBms;

    private Double stateOfChrgDisp;

    private Integer rapidChrgPort;

    private Integer normalChrgPort;

    private Double stateOfHealth;

    private Double btryMinTempr;

    private Double btryMaxTempr;

    private Double minCellVolt;

    private Double maxCellVolt;

    private String btryCellsArr;

    private String btryMdulTemprArr;

    private Double mvmnTime;


    public BatteryRecord(
            String carId,
            Double stateOfChrgBms,
            Double stateOfChrgDisp,
            Integer rapidChrgPort,
            Integer normalChrgPort,
            Double stateOfHealth,
            Double btryMinTempr,
            Double btryMaxTempr,
            Double minCellVolt,
            Double maxCellVolt,
            String btryCellsArr,
            String btryMdulTemprArr,
            Double mvmnTime)
    {
        this( 0 ,carId,stateOfChrgBms,stateOfChrgDisp,rapidChrgPort,
                normalChrgPort,  stateOfHealth,  btryMinTempr,
                btryMaxTempr,  minCellVolt,  maxCellVolt,  btryCellsArr,
                btryMdulTemprArr,  mvmnTime);
    }

    public BatteryRecord(int id, String carId, Double stateOfChrgBms, Double stateOfChrgDisp, Integer rapidChrgPort, Integer normalChrgPort, Double stateOfHealth, Double btryMinTempr, Double btryMaxTempr, Double minCellVolt, Double maxCellVolt, String btryCellsArr, String btryMdulTemprArr, Double mvmnTime) {
        this.id = id;
        this.carId = carId;
        this.stateOfChrgBms = stateOfChrgBms;
        this.stateOfChrgDisp = stateOfChrgDisp;
        this.rapidChrgPort = rapidChrgPort;
        this.normalChrgPort = normalChrgPort;
        this.stateOfHealth = stateOfHealth;
        this.btryMinTempr = btryMinTempr;
        this.btryMaxTempr = btryMaxTempr;
        this.minCellVolt = minCellVolt;
        this.maxCellVolt = maxCellVolt;
        this.btryCellsArr = btryCellsArr;
        this.btryMdulTemprArr = btryMdulTemprArr;
        this.mvmnTime = mvmnTime;
    }
}