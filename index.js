const AWS = require("aws-sdk");

if (process.env.NODE_ENV === 'devel') {
    const config = require('../config.js');
    AWS.config.update({
        accessKeyId: config.aws.accessKeyId,
        secretAccessKey: config.aws.secretAccessKey,
        region: "ap-northeast-2",
    });
} else {
    AWS.config.update({
        region: "ap-northeast-2",
    });
}

exports.handler = async (gevent, context) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    const event = JSON.parse(gevent.body || "{}");
    const count = event.count || 1000;
    let errors = null;
    if (!event.name && !event.device) {
        errors = {
            "status": "error",
            "name": ["이름(name)이 없습니다."]
        };
    }
    if (!event.lat) {
        errors = Object.assign(errors || {}, {
            "status": "error",
            "lat": ["위도(lat)가 없습니다."]
        });
    }
    if (!event.long) {
        errors = Object.assign(errors || {}, {
            "status": "error",
            "long": ["경도(long)가 없습니다."]
        });
    }
    if (!event.pin) {
        errors = Object.assign(errors || {}, {
            "status": "error",
            "pin": ["핀코드(pin)가 없습니다."]
        });
    }
    if (errors) {
        if (process.env.NODE_ENV === 'devel') {
            console.error(errors);
        }
        return {
            statusCode: 400,
            body: JSON.stringify(errors)
        }
    }
    if (count > 1000) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                "status": "error",
                "error": "요청 인자 count가 너무 큽니다."
            })
        };
    }
    const _southWest = event.sw;
    const _northEast = event.ne;
    let bounds = null;
    if (_southWest && _northEast) {
        const sw = _southWest.split(",");
        const ne = _northEast.split(",");
        bounds = [...ne, ...sw];
    }
    const params = {
        TableName: "CrowdSensorCloudDevice",
        Item: {
            "lat": event.lat,
            "long": event.long,
            "device": event.device || event.name,
            "pin": event.newPin || event.pin,
            "timestamp": new Date().getTime(),
            "address": event.address || '-',
            "manager": event.manager || '-',
            "space": event.space || "UNKNOWN",
            "modified": new Date().getTime(),
        }
    };
    try {
        const registerCallback = (err, data, resolve, reject) => {
            if (err) {
                reject("Unable to query. Error: " + JSON.stringify(err, null, 2));
            } else if (event.device && (!data.Items || data.Items.length == 0)) {
                resolve({
                    statusCode: 200,
                    body: JSON.stringify({
                        "status": "success",
                        "message": "정보가 등록(갱신) 되었습니다."
                    })
                });
            }
        };
        return await new Promise((resolve, reject) => {
            docClient.query({
                TableName: "CrowdSensorCloudDevice",
                ScanIndexForward: false,
                Limit: 1,
                KeyConditionExpression: "#n = :device",
                ExpressionAttributeNames: {
                    "#n": "device"
                },
                ExpressionAttributeValues: {
                    ":device": event.device || event.name
                }
            }, (err, data) => {
                if (err) {
                    reject("Unable to query. Error: " + JSON.stringify(err, null, 2));
                } else if (!data.Items || data.Items.length == 0) {
                    if (process.env.NODE_ENV === 'devel') {
                        console.log('새로운 레코드 입니다.');
                    }
                    docClient.put(params, (err, data) => registerCallback(err, data, resolve, reject));
                } else {
                    if (process.env.NODE_ENV === 'devel') {
                        console.log('기존의 레코드가 이미 있습니다.', data.Items[0]);
                    }
                    if (data.Items[0].pin === event.pin) {
                        if (data.Items[0].lat === params.Item.lat && data.Items[0].long === params.Item.long && data.Items[0].space === params.Item.space) {
                            docClient.update({
                                TableName: params.TableName,
                                Key: {
                                    "device": params.Item.device,
                                    "timestamp": data.Items[0].timestamp,
                                },
                                UpdateExpression: "set pin = :pin, address = :address, manager = :manager, modified = :modified",
                                ExpressionAttributeValues: {
                                    ":pin": params.Item.pin,
                                    ":address": params.Item.address || '',
                                    ":manager": params.Item.manager || '',
                                    ":modified": params.Item.modified
                                }
                            }, (err, data) => registerCallback(err, data, resolve, reject));
                        } else {
                            docClient.put(params, (err, data) => registerCallback(err, data, resolve, reject));
                        }
                    } else {
                        reject("PIN is not matched.");
                    }
                }
            });
        });
    } catch (err) {
        if (process.env.NODE_ENV === 'devel') {
            console.error(err);
        }
        return {
            statusCode: 400,
            body: JSON.stringify({
                "status": "error",
                "error": (typeof err === 'string') ? err : JSON.stringify(err)
            })
        };
    }
};


if (process.env.NODE_ENV === 'devel') {
    exports.handler({
        "body": JSON.stringify({
            name: "AirSensor00000000",
            lat: 37.611139,
            long: 126.996683,
            pin: 9999,
            newPin: 9999
        }),
        "resource": "/{proxy+}",
        "path": "/path/to/resource",
        "httpMethod": "POST",
        "isBase64Encoded": true,
        "queryStringParameters": {
            "ne": "37.69631767236258,127.23987579345705",
            "sw": "37.536954951447285,126.79664611816408"
        },
        "pathParameters": {
            "proxy": "/path/to/resource"
        },
        "stageVariables": {
            "baz": "qux"
        },
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Accept-Language": "en-US,en;q=0.8",
            "Cache-Control": "max-age=0",
            "CloudFront-Forwarded-Proto": "https",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-Mobile-Viewer": "false",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Tablet-Viewer": "false",
            "CloudFront-Viewer-Country": "US",
            "Host": "1234567890.execute-api.ap-northeast-2.amazonaws.com",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Custom User Agent String",
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "X-Amz-Cf-Id": "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "X-Forwarded-Port": "443",
            "X-Forwarded-Proto": "https"
        },
        "requestContext": {
            "accountId": "123456789012",
            "resourceId": "123456",
            "stage": "prod",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "requestTime": "09/Apr/2015:12:34:56 +0000",
            "requestTimeEpoch": 1428582896000,
            "identity": {
                "cognitoIdentityPoolId": null,
                "accountId": null,
                "cognitoIdentityId": null,
                "caller": null,
                "accessKey": null,
                "sourceIp": "127.0.0.1",
                "cognitoAuthenticationType": null,
                "cognitoAuthenticationProvider": null,
                "userArn": null,
                "userAgent": "Custom User Agent String",
                "user": null
            },
            "path": "/prod/path/to/resource",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "apiId": "1234567890",
            "protocol": "HTTP/1.1"
        }
    });
}
