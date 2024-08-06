const express = require('express');
const path = require('path');
const http = require('http');
const superagent = require('superagent');
const EventHubReader = require('./scripts/event-hub-reader.js');
const PersistStorage = require('./scripts/persist-storage');

const iotHubConnectionString = process.env.IotHubConnectionString;
if (!iotHubConnectionString) {
    console.error(`Environment variable IotHubConnectionString must be specified.`);
    return;
}
console.log(`Using IoT Hub connection string [${iotHubConnectionString}]`);

const eventHubConsumerGroup = process.env.EventHubConsumerGroup;
if (!eventHubConsumerGroup) {
    console.error(`Environment variable EventHubConsumerGroup must be specified.`);
    return;
}
console.log(`Using event hub consumer group [${eventHubConsumerGroup}]`);

const storageAccountName = process.env.StorageAccountName;
if (!storageAccountName) {
    console.error(`Environment variable StorageAccountName must be specified.`);
    return;
}

const storageAccountKey = process.env.StorageAccountAccessKey;
if (!storageAccountKey) {
    console.error(`Environment variable StorageAccountAccessKey must be specified.`);
    return;
}
console.log(`Using storage [${storageAccountName}], [${storageAccountKey}]`);

const clientAppPingUrl = process.env.ClientApplicationPingUrl;
console.log(`Using ping URL for client application [${clientAppPingUrl}]`);

function pingClientApp() {
    if (clientAppPingUrl) {
        superagent.get(clientAppPingUrl)
            .query({ issued: Date.now() })
            .end((err) => {
                const hasError = err ? 'yes' : 'no';
                console.log(`Completed ping operation. Has error: ${hasError}`);
            });
    }
}

setInterval(pingClientApp, 120 * 1000);

function returnSimpleText(req, res, next) {
    res.send(`This is just a stub site. This message is generated at [${Date.now()}]`);
}

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', returnSimpleText);
app.get('/', returnSimpleText);
app.use((req, res /* , next */) => {
  res.redirect('/');
});

const server = http.createServer(app);
server.listen(process.env.PORT || '3000', () => {
  console.log('Listening on %d.', server.address().port);
});

const eventHubReader = new EventHubReader(iotHubConnectionString, eventHubConsumerGroup);

const storage = new PersistStorage(storageAccountName, storageAccountKey, 'MeteostationMessages', 'MeteoData');
storage.connect();

(async () => {
    await eventHubReader.startReadMessage((message, date, deviceId) => {
        try {
            const payload = {
                IotData: message,
                MessageDate: date || Date.now().toISOString(),
                DeviceId: deviceId,
            };

            console.log('Received new message:');
            console.log(JSON.stringify(payload));

            storage.storeData(payload);
        } catch (err) {
            console.error('Error processing events: [%s] from [%s].', err, message);
        }
    });
})().catch();
