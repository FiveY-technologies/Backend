const express = require('express');
const axios = require('axios');
const mysql = require('mysql');
const app = express();
const port = 3000;
const cors = require('cors');

const apiUrl = 'https://api.vamosys.com/mobile/getGrpDataForTrustedClients?providerName=CALYX&fcode=SPVAMO';


app.use(cors());

const db = mysql.createPool({
  host: 'fivewhyrds.ctxjvxl0k0dq.us-east-1.rds.amazonaws.com',
  user: 'fivewhyadmin',
  password: 'Yayaya#143',
  database: '5ydatabase'
});



const createTableQuerysetInterval = `
CREATE TABLE IF NOT EXISTS setInterval (
       id INT NOT NULL,
       time TIMESTAMP,
        latitude VARCHAR(20),
        longitude VARCHAR(20),
        speed INT,
        date DATE,
        alert VARCHAR(5),
        direction VARCHAR(5),
        position VARCHAR(5),
        distanceCovered DECIMAL(10, 2),
        odoDistance DECIMAL(10, 2),
        odoMeterDevice INT,
        tankSize INT,
        deviceVolt DECIMAL(5, 2),
        status VARCHAR(5),
        altitude VARCHAR(5), 
        color VARCHAR(5),
        lastSeen DATETIME,
        ignitionStatus VARCHAR(5),
        insideGeoFence VARCHAR(5),
        isOverSpeed VARCHAR(5),
        address VARCHAR(255),
        parkedTime TIME,
        movingTime TIME,
        idleTime TIME,
        noDataTime TIME,
        alertDateTime TIME,
        latLngOld VARCHAR(50),
        loadTruck VARCHAR(50),
        loadTrailer VARCHAR(50),
        totalTruck VARCHAR(50),
        totalTrailer VARCHAR(50),
        vehicleBusy VARCHAR(5),
        fuelLitre DECIMAL(10, 2),
        temperature INT,
        powerStatus VARCHAR(20),
        deviceStatus INT,
        gsmLevel INT,
        startParkedTime TIME,
        endParkedTime TIME,
        maxSpeed INT,
        averageSpeed INT,
        lastComunicationTime TIME,
        isOutOfOrder VARCHAR(5),
        tripName VARCHAR(5),
        forwardOrBackward VARCHAR(5),
        enableLog VARCHAR(5),
        dateSec DATE,
        digitalInput3 VARCHAR(5),
        fuelSensorVolt VARCHAR(5),
        nTankSize DECIMAL(10, 2),
        noOfTank INT,
        sensorCount INT,
        gpsSimICCID VARCHAR(20),
        madeIn VARCHAR(20),
        manufacturingDate VARCHAR(20),
        chassisNumber VARCHAR(50),
        gpsSimNo VARCHAR(20),
        onboardDate VARCHAR(20),
        speedInfo INT,
        defaultMileage VARCHAR(5),
        noDataStatus INT,
        lat DECIMAL(10, 2),
        lng DECIMAL(10, 2),
        topSpeed INT,
        fuelSensorType VARCHAR(20),
        idleEndDate BIGINT,
        todayWorkingHours BIGINT,
        sensor VARCHAR(255) ,
        sensorvehiclemode VARCHAR(20),
        fuelLitres VARCHAR(10),
        vehicleMake VARCHAR(50),
        oprName VARCHAR(20),
        regNo VARCHAR(20),
        vehicleType VARCHAR(20),
        vehicleId VARCHAR(20),
        mobileNo VARCHAR(15),
        customMarker VARCHAR(20),
        deviceModel VARCHAR(20),
        shortName VARCHAR(20),
        orgId VARCHAR(20),
        overSpeedLimit INT,
        driverName VARCHAR(50),
        error VARCHAR(255),
        live VARCHAR(5),
        fuel VARCHAR(5),
        deviceId VARCHAR(50),
        expired VARCHAR(5),
        expiryDate VARCHAR(20),
        routeName VARCHAR(50),
        expiryDays INT,
        groupName VARCHAR(50),
        safetyParking VARCHAR(5),
        description VARCHAR(255),
        driverMobile VARCHAR(15),
        vehicleName VARCHAR(50),
        trackName VARCHAR(50),
        report VARCHAR(5),
        licenceExpiry VARCHAR(20),
        supportDetails VARCHAR(255),
        serial1 VARCHAR(5),
        expiryStatus VARCHAR(5),
        vehicleMode VARCHAR(20),
        vehicleModel VARCHAR(20),
        licenceType VARCHAR(20),
        licenceExpiryStatus VARCHAR(20),
        rigMode VARCHAR(20),
        secondaryEngineHours BIGINT,
        vehicleList VARCHAR(20),
        userId VARCHAR(20),
        macid VARCHAR(20),
        appid VARCHAR(20),
        groupvehicle VARCHAR(20),
        language VARCHAR(20),
        immobilizer VARCHAR(5),
        timeZone VARCHAR(50),
        calibrateMode VARCHAR(20),
        engineStatus VARCHAR(50),
        communicatingPortNo VARCHAR(10),
        isAsset BOOLEAN,
        vehicleTypeLabel VARCHAR(20),
        expectedFuelMileage DECIMAL(10, 2),
        immobilizerStatus VARCHAR(20),
        fcode VARCHAR(10),
        cameraEnabled BOOLEAN,
        ac BOOLEAN,
        end_fuel FLOAT,
        fuel_filling FLOAT,
        fuel_theft FLOAT,
        fuel_consumption FLOAT,
        current_day_fuel_cost DECIMAL (6,3),
      consumed_fuel_cost DECIMAL (6,3),
      end_kms FLOAT,
      distance_travelled FLOAT,
      kmpl FLOAT,
      liters_per_hour TIME,
        PRIMARY KEY(Id)
)
`;
const createSetHistoryTableData = `
CREATE TABLE IF NOT EXISTS alldatasInterhistory (
    HistoryId INT AUTO_INCREMENT PRIMARY KEY,
    id INT NOT NULL,
    change_time TIMESTAMP,
    latitude VARCHAR(20),
    longitude VARCHAR(20),
    speed INT,
    date BIGINT,
    alert VARCHAR(5),
    direction VARCHAR(5),
    position VARCHAR(5),
    distanceCovered DECIMAL(10, 2),
    odoDistance DECIMAL(10, 2),
    odoMeterDevice INT,
    tankSize INT,
    deviceVolt DECIMAL(5, 2),
    status VARCHAR(5),
    altitude DECIMAL(10, 2), 
    color VARCHAR(5),
    lastSeen DATETIME,
    ignitionStatus VARCHAR(5),
    insideGeoFence VARCHAR(5),
    isOverSpeed VARCHAR(5),
    address VARCHAR(255),
    parkedTime INT,
    movingTime INT,
    idleTime INT,
    noDataTime INT,
    alertDateTime VARCHAR(5),
    latLngOld VARCHAR(50),
    loadTruck VARCHAR(50),
    loadTrailer VARCHAR(50),
    totalTruck VARCHAR(50),
    totalTrailer VARCHAR(50),
    vehicleBusy VARCHAR(5),
    fuelLitre DECIMAL(10, 2),
    temperature INT,
    powerStatus VARCHAR(20),
    deviceStatus INT,
    gsmLevel INT,
    startParkedTime INT,
    endParkedTime INT,
    maxSpeed INT,
    averageSpeed INT,
    lastComunicationTime BIGINT,
    isOutOfOrder VARCHAR(5),
    tripName VARCHAR(5),
    forwardOrBackward VARCHAR(5),
    enableLog VARCHAR(5),
    dateSec BIGINT,
    digitalInput3 VARCHAR(5),
    fuelSensorVolt VARCHAR(5),
    nTankSize DECIMAL(10, 2),
    noOfTank INT,
    sensorCount INT,
    gpsSimICCID VARCHAR(20),
    madeIn VARCHAR(20),
    manufacturingDate VARCHAR(20),
    chassisNumber VARCHAR(50),
    gpsSimNo VARCHAR(20),
    onboardDate VARCHAR(20),
    speedInfo INT,
    defaultMileage VARCHAR(5),
    noDataStatus INT,
    lat DECIMAL(10, 2),
    lng DECIMAL(10, 2),
    topSpeed INT,
    fuelSensorType VARCHAR(20),
    idleEndDate BIGINT,
    todayWorkingHours BIGINT,
    sensor VARCHAR(255) ,
    sensorvehiclemode VARCHAR(20),
    fuelLitres VARCHAR(10),
    vehicleMake VARCHAR(50),
    oprName VARCHAR(20),
    regNo VARCHAR(20),
    vehicleType VARCHAR(20),
    vehicleId VARCHAR(20),
    mobileNo VARCHAR(15),
    customMarker VARCHAR(20),
    deviceModel VARCHAR(20),
    shortName VARCHAR(20),
    orgId VARCHAR(20),
    overSpeedLimit INT,
    driverName VARCHAR(50),
    error VARCHAR(255),
    live VARCHAR(5),
    fuel VARCHAR(5),
    deviceId VARCHAR(50),
    expired VARCHAR(5),
    expiryDate VARCHAR(20),
    routeName VARCHAR(50),
    expiryDays INT,
    groupName VARCHAR(50),
    safetyParking VARCHAR(5),
    description VARCHAR(255),
    driverMobile VARCHAR(15),
    vehicleName VARCHAR(50),
    trackName VARCHAR(50),
    report VARCHAR(5),
    licenceExpiry VARCHAR(20),
    supportDetails VARCHAR(255),
    serial1 VARCHAR(5),
    expiryStatus VARCHAR(5),
    vehicleMode VARCHAR(20),
    vehicleModel VARCHAR(20),
    licenceType VARCHAR(20),
    licenceExpiryStatus VARCHAR(20),
    rigMode VARCHAR(20),
    secondaryEngineHours BIGINT,
    vehicleList VARCHAR(20),
    userId VARCHAR(20),
    macid VARCHAR(20),
    appid VARCHAR(20),
    groupvehicle VARCHAR(20),
    language VARCHAR(20),
    immobilizer VARCHAR(5),
    timeZone VARCHAR(50),
    calibrateMode VARCHAR(20),
    engineStatus VARCHAR(50),
    communicatingPortNo VARCHAR(10),
    isAsset BOOLEAN,
    vehicleTypeLabel VARCHAR(20),
    expectedFuelMileage DECIMAL(10, 2),
    immobilizerStatus VARCHAR(20),
    fcode VARCHAR(10),
    cameraEnabled BOOLEAN,
    ac BOOLEAN,
    end_fuel INT,
    fuel_filling FLOAT,
    fuel_theft FLOAT,
    fuel_consumption FLOAT,
    current_day_fuel_cost DECIMAL (6,3),
  consumed_fuel_cost DECIMAL (6,3),
  end_kms FLOAT,
  distance_travelled FLOAT,
  kmpl FLOAT,
  liters_per_hour TIME
    
)
`;
db.getConnection((connectionError, connection) => {
    if (connectionError) {
      console.error('Database connection failed: ' + connectionError.stack);
      return;
    }
  
    connection.query(createTableQuerysetInterval, (err) => {
       
      if (err) {
        console.error('Error creating the table: ' + err);
      } else {
        console.log('setInterval  table created ');
      }
    });
    connection.query(createSetHistoryTableData, (err) => {
      connection.release(); 
  
      if (err) {
        console.error('Error creating the table: ' + err);
      } else {
        console.log('All datas Interhistory table created ');
      }
    });
});
function fetchDataAndSend()  {
    function currenttime() {
        const now = new Date();
        const hours = now.getHours().toString().padStart(2, '0');
        const minutes = now.getMinutes().toString().padStart(2, '0');
        const seconds = now.getSeconds().toString().padStart(2, '0');
        return `${hours}:${minutes}:${seconds}`;
      }
      
      // 
    axios.get(apiUrl)
      .then((response) => {
        const apiData = response.data;
        let queriesToExecute = apiData.length;
  
        db.getConnection((connectionError, connection) => {
          if (connectionError) {
            console.error('Database connection failed: ' + connectionError.stack);
            return res.status(500).json({ error: 'Database connection failed' });
          }
  
          for (const item of apiData) {
            const checkQuery = 'SELECT id FROM setInterval WHERE id = ?';
  
            connection.query(checkQuery, [item.rowId], (err, results) => {
              if (err) {
                console.error('Error checking data: ' + err);
              } else if (results.length === 0) { 
  
                const end_fuel = '20';
                const fuel_filling = '60';
                const fuel_consumption = item.fuelLitre + fuel_filling - end_fuel;
                const fuel_theft  = fuel_consumption + fuel_filling;
                const current_day_fuel_cost = '103.39';
                const consumed_fuel_cost = fuel_consumption * current_day_fuel_cost;
                const end_kms = '3500';
                const distance_travelled = item.odoDistance - end_kms;
                const kmpl = distance_travelled / fuel_consumption;
                const liters_per_hour = '2';
  
                  const values = [
                      item.rowId,item.latitude,  item.longitude,  item.speed,  item.date,
                      item.alert, item.direction, item.position,  item.distanceCovered,
                      item.odoDistance,  item.odoMeterDevice,  item.tankSize,  item.deviceVolt,
                      item.status,  item.altitude, item.color, item.lastSeen,
                      item.ignitionStatus,  item.insideGeoFence,  item.isOverSpeed,
                      item.address,  item.parkedTime,  item.movingTime,  item.idleTime,
                      item.noDataTime,  item.alertDateTime,  item.latLngOld, item.loadTruck,
                      item.loadTrailer, item.totalTruck, item.totalTrailer,
                      item.vehicleBusy,  item.fuelLitre,  item.temperature, item.powerStatus,
                      item.deviceStatus, item.gsmLevel,  item.startParkedTime,  item.endParkedTime,
                      item.maxSpeed,  item.averageSpeed,  item.lastComunicationTime,
                      item.isOutOfOrder, item.tripName, item.forwardOrBackward, item.enableLog,
                      item.dateSec,item.digitalInput3,item.fuelSensorVolt,  item.nTankSize,
                      item.noOfTank,  item.sensorCount, item.gpsSimICCID, item.madeIn,
                      item.manufacturingDate, item.chassisNumber, item.gpsSimNo,
                      item.onboardDate, item.speedInfo, item.defaultMileage,  item.noDataStatus,
                      item.lat,  item.lng,  item.topSpeed, item.fuelSensorType,
                      item.idleEndDate,  item.todayWorkingHours, item.sensorBasedVehicleMode[0].sensor,
                      item.sensorBasedVehicleMode[0].vehicleMode, 
                      item.fuelLitres, item.vehicleMake,
                      item.oprName,  item.regNo, item.vehicleType, item.vehicleId,
                      item.mobileNo,  item.customMarker,  item.deviceModel,  item.shortName,
                      item.orgId,  item.overSpeedLimit, item.driverName, item.error,
                      item.live, item.fuel,  item.deviceId, item.expired,
                      item.expiryDate, item.routeName,  item.expiryDays, item.groupName,
                      item.safetyParking, item.description,  item.driverMobile,  item.vehicleName,
                      item.trackName, item.report, item.licenceExpiry,  item.supportDetails,
                      item.serial1, item.expiryStatus, item.vehicleMode,item.vehicleModel, item.licenceType,
                      item.licenceExpiryStatus, item.rigMode,  item.secondaryEngineHours,
                      item.vehicleList, item.userId, item.macid, item.appid,
                      item.group, item.language, item.immobilizer, item.timeZone,
                      item.calibrateMode, item.engineStatus,  item.communicatingPortNo,
                      item.isAsset, item.vehicleTypeLabel,  item.expectedFuelMileage,
                      item.immobilizerStatus, item.fcode,  item.cameraEnabled,  item.ac,end_fuel,fuel_filling,
                      fuel_consumption,fuel_theft,current_day_fuel_cost,consumed_fuel_cost,
                      end_kms,distance_travelled,kmpl,liters_per_hour
                    ];
  
                    const insertQuery =
                    `
                    INSERT INTO setInterval (
                        id, time, latitude, longitude, speed, date, alert, direction, position, distanceCovered, odoDistance, odoMeterDevice,
                        tankSize, deviceVolt, status, altitude, color, lastSeen, ignitionStatus, insideGeoFence, isOverSpeed,
                        address, parkedTime, movingTime, idleTime, noDataTime, alertDateTime, latLngOld, loadTruck, loadTrailer,
                        totalTruck, totalTrailer, vehicleBusy, fuelLitre, temperature, powerStatus, deviceStatus, gsmLevel,
                        startParkedTime, endParkedTime, maxSpeed, averageSpeed, lastComunicationTime, isOutOfOrder, tripName,
                        forwardOrBackward, enableLog, dateSec, digitalInput3, fuelSensorVolt, nTankSize, noOfTank, sensorCount,
                        gpsSimICCID, madeIn, manufacturingDate, chassisNumber, gpsSimNo, onboardDate, speedInfo, defaultMileage,
                        noDataStatus, lat, lng, topSpeed, fuelSensorType, idleEndDate, todayWorkingHours, sensor, sensorvehiclemode,
                        fuelLitres, vehicleMake, oprName, regNo, vehicleType, vehicleId, mobileNo, customMarker, deviceModel, shortName,
                        orgId, overSpeedLimit, driverName, error, live, fuel, deviceId, expired, expiryDate, routeName, expiryDays,
                        groupName, safetyParking, description, driverMobile, vehicleName, trackName, report, licenceExpiry,
                        supportDetails, serial1, expiryStatus,vehicleMode,vehicleModel, licenceType, licenceExpiryStatus, rigMode,
                        secondaryEngineHours, vehicleList, userId, macid, appid, groupvehicle, language, immobilizer, timeZone,
                        calibrateMode, engineStatus, communicatingPortNo, isAsset, vehicleTypeLabel, expectedFuelMileage,
                        immobilizerStatus, fcode, cameraEnabled, ac ,end_fuel,fuel_filling,fuel_consumption,fuel_theft,
                        current_day_fuel_cost,consumed_fuel_cost,end_kms,distance_travelled,kmpl,liters_per_hour
                    ) 
                    VALUES (
                        ?,CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'), ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?
                    )
                    `;
                    
                connection.query(insertQuery, values, (err) => {
                  if (err) {
                    console.error('Error inserting data: ' + err);
                  } else {
                    console.log('Data inserted successfully');
                     logChange(connection,item.rowId, item.latitude,  item.longitude,  item.speed,  item.date,
                      item.alert, item.direction, item.position,  item.distanceCovered,
                       item.odoDistance,  item.odoMeterDevice,  item.tankSize,  item.deviceVolt,
                       item.status,  item.altitude, item.color, item.lastSeen,
                      item.ignitionStatus,  item.insideGeoFence,  item.isOverSpeed,
                      item.address,  item.parkedTime,  item.movingTime,  item.idleTime,
                      item.noDataTime,  item.alertDateTime,  item.latLngOld, item.loadTruck,
                       item.loadTrailer, item.totalTruck, item.totalTrailer,
                       item.vehicleBusy,  item.fuelLitre,  item.temperature, item.powerStatus,
                       item.deviceStatus, item.gsmLevel,  item.startParkedTime,  item.endParkedTime,
                       item.maxSpeed,  item.averageSpeed,  item.lastComunicationTime,
                      item.isOutOfOrder, item.tripName, item.forwardOrBackward, item.enableLog,
                      item.dateSec,item.digitalInput3,item.fuelSensorVolt,  item.nTankSize,
                       item.noOfTank,  item.sensorCount, item.gpsSimICCID, item.madeIn,
                     item.manufacturingDate, item.chassisNumber, item.gpsSimNo,
                     item.onboardDate, item.speedInfo, item.defaultMileage,  item.noDataStatus,
                       item.lat,  item.lng,  item.topSpeed, item.fuelSensorType,
                      item.idleEndDate,  item.todayWorkingHours, item.sensorBasedVehicleMode[0].sensor,
                      item.sensorBasedVehicleMode[0].vehicleMode, 
                      item.fuelLitres, item.vehicleMake,
                     item.oprName,  item.regNo, item.vehicleType, item.vehicleId,
                      item.mobileNo,  item.customMarker,  item.deviceModel,  item.shortName,
                      item.orgId,  item.overSpeedLimit, item.driverName, item.error,
                      item.live, item.fuel,  item.deviceId, item.expired,
                      item.expiryDate, item.routeName,  item.expiryDays, item.groupName,
                       item.safetyParking, item.description,  item.driverMobile,  item.vehicleName,
                       item.trackName, item.report, item.licenceExpiry,  item.supportDetails,
                       item.serial1, item.expiryStatus, item.vehicleMode,item.vehicleModel, item.licenceType,
                       item.licenceExpiryStatus, item.rigMode,  item.secondaryEngineHours,
                       item.vehicleList, item.userId, item.macid, item.appid,
                      item.group, item.language, item.immobilizer, item.timeZone,
                      item.calibrateMode, item.engineStatus,  item.communicatingPortNo,
                      item.isAsset, item.vehicleTypeLabel,  item.expectedFuelMileage,
                      item.immobilizerStatus, item.fcode,  item.cameraEnabled,  item.ac,end_fuel,fuel_filling,
                       fuel_consumption,fuel_theft,current_day_fuel_cost,consumed_fuel_cost,
                       end_kms,distance_travelled,kmpl,liters_per_hour);
                  }

                  queriesToExecute--;
                  if (queriesToExecute === 0) {
                    connection.release(); 
                    fetchAndSendData(res);
                  }
                });
              } else {
                const end_fuel = '20';
                const fuel_filling = '60';
                const fuel_consumption = item.fuelLitre + fuel_filling - end_fuel;
                const fuel_theft  = fuel_consumption + fuel_filling;
                const current_day_fuel_cost = '103.39';
                const consumed_fuel_cost = fuel_consumption * current_day_fuel_cost;
                const end_kms = '3500';
                const distance_travelled = item.odoDistance - end_kms;
                const kmpl = distance_travelled / fuel_consumption;
                const liters_per_hour='2';
             
  
          const updateQuery = `
                UPDATE setInterval SET 
                time=CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'),latitude =?, longitude=?, speed=?, date=?, alert=?, direction=?, position=?, distanceCovered=?, odoDistance=?, odoMeterDevice=?,
                tankSize=?, deviceVolt=?, status=?, altitude=?, color=?, lastSeen=?, ignitionStatus=?, insideGeoFence=?, isOverSpeed=?,
                address=?, parkedTime=?, movingTime=?, idleTime=?, noDataTime=?, alertDateTime=?, latLngOld=?, loadTruck=?, loadTrailer=?,
                totalTruck=?, totalTrailer=?, vehicleBusy=?, fuelLitre=?, temperature=?, powerStatus=?, deviceStatus=?, gsmLevel=?,
                startParkedTime=?, endParkedTime=?, maxSpeed=?, averageSpeed=?, lastComunicationTime=?, isOutOfOrder=?, tripName=?,
                forwardOrBackward=?, enableLog=?, dateSec=?, digitalInput3=?, fuelSensorVolt=?, nTankSize=?, noOfTank=?, sensorCount=?,
                gpsSimICCID=?, madeIn=?, manufacturingDate=?, chassisNumber=?, gpsSimNo=?, onboardDate=?, speedInfo=?, defaultMileage=?,
                noDataStatus=?, lat=?, lng=?, topSpeed=?, fuelSensorType=?, idleEndDate=?, todayWorkingHours=?, sensor=?, sensorvehiclemode=?,
                fuelLitres=?, vehicleMake=?, oprName=?, regNo=?, vehicleType=?, vehicleId=?, mobileNo=?, customMarker=?, deviceModel=?, shortName=?,
                orgId=?, overSpeedLimit=?, driverName=?, error=?, live=?, fuel=?, deviceId=?, expired=?, expiryDate=?, routeName=?, expiryDays=?,
                groupName=?, safetyParking=?, description=?, driverMobile=?, vehicleName=?, trackName=?, report=?, licenceExpiry=?,
                supportDetails=?, serial1=?, expiryStatus=?,vehicleMode=?, vehicleModel=?, licenceType=?, licenceExpiryStatus=?, rigMode=?,
                secondaryEngineHours=?, vehicleList=?, userId=?, macid=?, appid=?, groupvehicle=?, language=?, immobilizer=?, timeZone=?,
                calibrateMode=?, engineStatus=?, communicatingPortNo=?, isAsset=?, vehicleTypeLabel=?, expectedFuelMileage=?,
                immobilizerStatus=?, fcode=?, cameraEnabled=?, ac=?,end_fuel=?,fuel_filling=? ,fuel_consumption=?,fuel_theft=?,
                current_day_fuel_cost=?,consumed_fuel_cost=?,end_kms=?,
                distance_travelled =?, kmpl=? ,liters_per_hour=? WHERE id = ?`;
  
                const updateValues = [
                  item.latitude,  item.longitude,  item.speed,  item.date,
                      item.alert, item.direction, item.position,  item.distanceCovered,
                      item.odoDistance,  item.odoMeterDevice,  item.tankSize,  item.deviceVolt,
                      item.status,  item.altitude, item.color, item.lastSeen,
                      item.ignitionStatus,  item.insideGeoFence,  item.isOverSpeed,
                      item.address,  item.parkedTime,  item.movingTime,  item.idleTime,
                      item.noDataTime,  item.alertDateTime,  item.latLngOld, item.loadTruck,
                      item.loadTrailer, item.totalTruck, item.totalTrailer,
                      item.vehicleBusy,  item.fuelLitre,  item.temperature, item.powerStatus,
                      item.deviceStatus, item.gsmLevel,  item.startParkedTime,  item.endParkedTime,
                      item.maxSpeed,  item.averageSpeed,  item.lastComunicationTime,
                      item.isOutOfOrder, item.tripName, item.forwardOrBackward, item.enableLog,
                      item.dateSec,item.digitalInput3,item.fuelSensorVolt,  item.nTankSize,
                      item.noOfTank,  item.sensorCount, item.gpsSimICCID, item.madeIn,
                      item.manufacturingDate, item.chassisNumber, item.gpsSimNo,
                      item.onboardDate, item.speedInfo, item.defaultMileage,  item.noDataStatus,
                      item.lat,  item.lng,  item.topSpeed, item.fuelSensorType,
                      item.idleEndDate,  item.todayWorkingHours, item.sensorBasedVehicleMode[0].sensor,
                      item.sensorBasedVehicleMode[0].vehicleMode, 
                      item.fuelLitres, item.vehicleMake,
                      item.oprName,  item.regNo, item.vehicleType, item.vehicleId,
                      item.mobileNo,  item.customMarker,  item.deviceModel,  item.shortName,
                      item.orgId,  item.overSpeedLimit, item.driverName, item.error,
                      item.live, item.fuel,  item.deviceId, item.expired,
                      item.expiryDate, item.routeName,  item.expiryDays, item.groupName,
                      item.safetyParking, item.description,  item.driverMobile,  item.vehicleName,
                      item.trackName, item.report, item.licenceExpiry,  item.supportDetails,
                      item.serial1, item.expiryStatus,item.vehicleMode, item.vehicleModel, item.licenceType,
                      item.licenceExpiryStatus, item.rigMode,  item.secondaryEngineHours,
                      item.vehicleList, item.userId, item.macid, item.appid,
                      item.group, item.language, item.immobilizer, item.timeZone,
                      item.calibrateMode, item.engineStatus,  item.communicatingPortNo,
                      item.isAsset, item.vehicleTypeLabel,  item.expectedFuelMileage,
                      item.immobilizerStatus, item.fcode,  item.cameraEnabled,  item.ac,
                      end_fuel,fuel_filling,fuel_consumption,fuel_theft,current_day_fuel_cost,consumed_fuel_cost,
                      end_kms,distance_travelled,kmpl,liters_per_hour,item.rowId   
                     
              ];
                
                connection.query(updateQuery, updateValues, (err) => {
                  if (err) {
                    console.error('Error updating data: ' + err); 
                }else {
                     console.log(currenttime(), 'Data updated successfully');

                     logChange(connection,item.rowId, item.latitude,  item.longitude,  item.speed,  item.date,
                       item.alert, item.direction, item.position,  item.distanceCovered,
                       item.odoDistance,  item.odoMeterDevice,  item.tankSize,  item.deviceVolt,
                       item.status,  item.altitude, item.color, item.lastSeen,
                       item.ignitionStatus,  item.insideGeoFence,  item.isOverSpeed,
                       item.address,  item.parkedTime,  item.movingTime,  item.idleTime,
                       item.noDataTime,  item.alertDateTime,  item.latLngOld, item.loadTruck,
                       item.loadTrailer, item.totalTruck, item.totalTrailer,
                       item.vehicleBusy,  item.fuelLitre,  item.temperature, item.powerStatus,
                       item.deviceStatus, item.gsmLevel,  item.startParkedTime,  item.endParkedTime,
                       item.maxSpeed,  item.averageSpeed,  item.lastComunicationTime,
                       item.isOutOfOrder, item.tripName, item.forwardOrBackward, item.enableLog,
                       item.dateSec,item.digitalInput3,item.fuelSensorVolt,  item.nTankSize,
                       item.noOfTank,  item.sensorCount, item.gpsSimICCID, item.madeIn,
                       item.manufacturingDate, item.chassisNumber, item.gpsSimNo,
                       item.onboardDate, item.speedInfo, item.defaultMileage,  item.noDataStatus,
                       item.lat,  item.lng,  item.topSpeed, item.fuelSensorType,
                       item.idleEndDate,  item.todayWorkingHours, item.sensorBasedVehicleMode[0].sensor,
                       item.sensorBasedVehicleMode[0].vehicleMode, 
                       item.fuelLitres, item.vehicleMake,
                       item.oprName,  item.regNo, item.vehicleType, item.vehicleId,
                      item.mobileNo,  item.customMarker,  item.deviceModel,  item.shortName,
                       item.orgId,  item.overSpeedLimit, item.driverName, item.error,
                       item.live, item.fuel,  item.deviceId, item.expired,
                       item.expiryDate, item.routeName,  item.expiryDays, item.groupName,
                       item.safetyParking, item.description,  item.driverMobile,  item.vehicleName,
                       item.trackName, item.report, item.licenceExpiry,  item.supportDetails,
                       item.serial1, item.expiryStatus, item.vehicleMode,item.vehicleModel, item.licenceType,
                       item.licenceExpiryStatus, item.rigMode,  item.secondaryEngineHours,
                       item.vehicleList, item.userId, item.macid, item.appid,
                       item.group, item.language, item.immobilizer, item.timeZone,
                       item.calibrateMode, item.engineStatus,  item.communicatingPortNo,
                       item.isAsset, item.vehicleTypeLabel,  item.expectedFuelMileage,
                     item.immobilizerStatus, item.fcode,  item.cameraEnabled,  item.ac,
                       end_fuel,fuel_filling,fuel_consumption,fuel_theft,current_day_fuel_cost,consumed_fuel_cost,
                       end_kms,distance_travelled,kmpl,liters_per_hour,
                        );
                  }
  

                  queriesToExecute--;
                  if (queriesToExecute === 0) {
                    connection.release(); 
                    console.log(currenttime(), 'Data updated successfully'); 
                  }
                }
                );

              }

            });
          }
        });
      })
      .catch((error) => {
        console.error('Error fetching data from the API: ' + error);
        res.status(500).json({ error: 'Error fetching data from the API' });
      });
  };
  fetchDataAndSend();

  const interval = 120000; // 2 minutes
setInterval(fetchDataAndSend, interval);

app.get('/alldatas', (req, res) => {
  fetchAndSendData(res);
});
app.get('/alldatas/history', (req, res) => {
  const fromDate = req.query.fromDate;
  const toDate = req.query.toDate;
  
  db.getConnection((connectionError, connection) => {
    if (connectionError) {
      console.error('Database connection failed: ' + connectionError.stack);
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const selectsetHistoryQuery = 'SELECT * FROM alldatasInterhistory';
    const values = [fromDate, toDate]

    connection.query(selectsetHistoryQuery, values,(err, results) => {
      if (err) {
        console.error('Error fetching data from the alldatasInterhistory table: ' + err);
        res.status(500).json({ error: 'Error fetching data from the alldatasInterhistory table' });
      } else {
        
        connection.release();
        res.json(results);
      }
    });
  });
});

function logChange(connection, id, latitude, longitude, speed, date, alert, direction, position, distanceCovered, odoDistance, odoMeterDevice,
  tankSize, deviceVolt, status, altitude, color, lastSeen, ignitionStatus, insideGeoFence, isOverSpeed,
  address, parkedTime, movingTime, idleTime, noDataTime, alertDateTime, latLngOld, loadTruck, loadTrailer,
  totalTruck, totalTrailer, vehicleBusy, fuelLitre, temperature, powerStatus, deviceStatus, gsmLevel,
  startParkedTime, endParkedTime, maxSpeed, averageSpeed, lastComunicationTime, isOutOfOrder, tripName,
  forwardOrBackward, enableLog, dateSec, digitalInput3, fuelSensorVolt, nTankSize, noOfTank, sensorCount,
  gpsSimICCID, madeIn, manufacturingDate, chassisNumber, gpsSimNo, onboardDate, speedInfo, defaultMileage,
  noDataStatus, lat, lng, topSpeed, fuelSensorType, idleEndDate, todayWorkingHours, sensor, sensorvehiclemode,
  fuelLitres, vehicleMake, oprName, regNo, vehicleType, vehicleId, mobileNo, customMarker, deviceModel, shortName,
  orgId, overSpeedLimit, driverName, error, live, fuel, deviceId, expired, expiryDate, routeName, expiryDays,
  groupName, safetyParking, description, driverMobile, vehicleName, trackName, report, licenceExpiry,
  supportDetails, serial1, expiryStatus,vehicleMode,vehicleModel, licenceType, licenceExpiryStatus, rigMode,
  secondaryEngineHours, vehicleList, userId, macid, appid, groupvehicle, language, immobilizer, timeZone,
  calibrateMode, engineStatus, communicatingPortNo, isAsset, vehicleTypeLabel, expectedFuelMileage,
  immobilizerStatus, fcode, cameraEnabled, ac,
  end_fuel,fuel_filling,fuel_consumption,fuel_theft,current_day_fuel_cost,consumed_fuel_cost,
  end_kms,distance_travelled,kmpl,liters_per_hour,change_time
  )
 
   {

   

  const logQuery = ` INSERT INTO alldatasInterhistory (
    id, latitude, longitude, speed, date, alert, direction, position, distanceCovered, odoDistance, odoMeterDevice,
    tankSize, deviceVolt, status, altitude, color, lastSeen, ignitionStatus, insideGeoFence, isOverSpeed,
    address, parkedTime, movingTime, idleTime, noDataTime, alertDateTime, latLngOld, loadTruck, loadTrailer,
    totalTruck, totalTrailer, vehicleBusy, fuelLitre, temperature, powerStatus, deviceStatus, gsmLevel,
    startParkedTime, endParkedTime, maxSpeed, averageSpeed, lastComunicationTime, isOutOfOrder, tripName,
    forwardOrBackward, enableLog, dateSec, digitalInput3, fuelSensorVolt, nTankSize, noOfTank, sensorCount,
    gpsSimICCID, madeIn, manufacturingDate, chassisNumber, gpsSimNo, onboardDate, speedInfo, defaultMileage,
    noDataStatus, lat, lng, topSpeed, fuelSensorType, idleEndDate, todayWorkingHours, sensor, sensorvehiclemode,
    fuelLitres, vehicleMake, oprName, regNo, vehicleType, vehicleId, mobileNo, customMarker, deviceModel, shortName,
    orgId, overSpeedLimit, driverName, error, live, fuel, deviceId, expired, expiryDate, routeName, expiryDays,
    groupName, safetyParking, description, driverMobile, vehicleName, trackName, report, licenceExpiry,
    supportDetails, serial1, expiryStatus,vehicleMode,vehicleModel, licenceType, licenceExpiryStatus, rigMode,
    secondaryEngineHours, vehicleList, userId, macid, appid, groupvehicle, language, immobilizer, timeZone,
    calibrateMode, engineStatus, communicatingPortNo, isAsset, vehicleTypeLabel, expectedFuelMileage,
    immobilizerStatus, fcode, cameraEnabled, ac ,
    end_fuel,fuel_filling,fuel_consumption,fuel_theft,current_day_fuel_cost,consumed_fuel_cost,
    end_kms,distance_travelled,kmpl,liters_per_hour,change_time
 
) 
  
 

VALUES (
  ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
  ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
  ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
  ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
  ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
  ?,?,?,?,?,?,?,?,?,?,CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')
 

) 
 
 `;

 
 
  const logValues = [ id, latitude, longitude, speed, date, alert, direction, position, distanceCovered, odoDistance, odoMeterDevice,
    tankSize, deviceVolt, status, altitude, color, lastSeen, ignitionStatus, insideGeoFence, isOverSpeed,
    address, parkedTime, movingTime, idleTime, noDataTime, alertDateTime, latLngOld, loadTruck, loadTrailer,
    totalTruck, totalTrailer, vehicleBusy, fuelLitre, temperature, powerStatus, deviceStatus, gsmLevel,
    startParkedTime, endParkedTime, maxSpeed, averageSpeed, lastComunicationTime, isOutOfOrder, tripName,
    forwardOrBackward, enableLog, dateSec, digitalInput3, fuelSensorVolt, nTankSize, noOfTank, sensorCount,
    gpsSimICCID, madeIn, manufacturingDate, chassisNumber, gpsSimNo, onboardDate, speedInfo, defaultMileage,
    noDataStatus, lat, lng, topSpeed, fuelSensorType, idleEndDate, todayWorkingHours, sensor, sensorvehiclemode,
    fuelLitres, vehicleMake, oprName, regNo, vehicleType, vehicleId, mobileNo, customMarker, deviceModel, shortName,
    orgId, overSpeedLimit, driverName, error, live, fuel, deviceId, expired, expiryDate, routeName, expiryDays,
    groupName, safetyParking, description, driverMobile, vehicleName, trackName, report, licenceExpiry,
    supportDetails, serial1, expiryStatus,vehicleMode,vehicleModel, licenceType, licenceExpiryStatus, rigMode,
    secondaryEngineHours, vehicleList, userId, macid, appid, groupvehicle, language, immobilizer, timeZone,
    calibrateMode, engineStatus, communicatingPortNo, isAsset, vehicleTypeLabel, expectedFuelMileage,
    immobilizerStatus, fcode, cameraEnabled, ac,
    end_fuel,fuel_filling,fuel_consumption,fuel_theft,current_day_fuel_cost,consumed_fuel_cost,
    end_kms,distance_travelled,kmpl,liters_per_hour,change_time
  ];

  connection.query(logQuery, logValues, (err) => {
    if (err) {
      console.error('Error in logging alldatasInterhistory: ' + err);
    } else {
      console.log('Data updated in alldatasInterhistory table successfully');
    }
  });
  
}


  function fetchAndSendData(res) {
    db.getConnection((connectionError, connection) => {
      if (connectionError) {
        console.error('Database connection failed: ' + connectionError.stack);
        return res.status(500).json({ error: 'Database connection failed' });
      }
  
      const selectQuery = 'SELECT * FROM setInterval';
  
      connection.query(selectQuery, (err, results) => {
        if (err) {
          console.error('Error fetching data from the alldatas table: ' + err);
          res.status(500).json({ error: 'Error fetching data from the alldatas table' });
        } else {
          res.json(results);
        }
      });
    });
  }
  
  app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
  });
  
    
