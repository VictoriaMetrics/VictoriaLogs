package logstorage

import (
	"testing"
	"time"
)

func TestSyslogParser(t *testing.T) {
	f := func(s string, timezone *time.Location, resultExpected string) {
		t.Helper()

		const currentYear = 2024
		p := GetSyslogParser(currentYear, timezone)
		defer PutSyslogParser(p)

		p.Parse(s)
		result := MarshalFieldsToLogfmt(nil, p.Fields)
		if string(result) != resultExpected {
			t.Fatalf("unexpected result when parsing [%s]; got\n%s\nwant\n%s\n", s, result, resultExpected)
		}
	}

	// RFC 3164
	f("Jun  3 12:08:33 abcd systemd[1]: Starting Update the local ESM caches...", time.UTC,
		`format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=systemd proc_id=1 message="Starting Update the local ESM caches..."`)
	f("<165>Jun  3 12:08:33 abcd systemd[1]: Starting Update the local ESM caches...", time.UTC,
		`priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=systemd proc_id=1 message="Starting Update the local ESM caches..."`)
	f("Mar 13 12:08:33 abcd systemd: Starting Update the local ESM caches...", time.UTC,
		`format=rfc3164 timestamp=2024-03-13T12:08:33Z hostname=abcd app_name=systemd message="Starting Update the local ESM caches..."`)
	f("Jun  3 12:08:33 abcd - Starting Update the local ESM caches...", time.UTC,
		`format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=- message="Starting Update the local ESM caches..."`)
	f("Jun  3 12:08:33 - - Starting Update the local ESM caches...", time.UTC,
		`format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=- app_name=- message="Starting Update the local ESM caches..."`)

	// RFC 3164: missing hostname, first token is tag (FreeBSD syslogd over UDP)
	f("Jun  3 12:08:33 sshd-session[14308]: Received disconnect from 192.168.0.1 port 22:11: disconnected by user", time.UTC,
		`format=rfc3164 timestamp=2024-06-03T12:08:33Z app_name=sshd-session proc_id=14308 message="Received disconnect from 192.168.0.1 port 22:11: disconnected by user"`)
	f("Jun  3 12:08:33 sshd-session: foo", time.UTC,
		`format=rfc3164 timestamp=2024-06-03T12:08:33Z app_name=sshd-session message=foo`)

	// RFC 5424
	f(`<134>1 2024-12-09T18:25:35.401631+00:00 ps999 account-server - - [sd@51059 project="secret" ] 1.2.3.4 - - [09/Dec/2024:18:25:35 +0000] "PUT someurl" 201 - "-" "-" "container-updater 1283500" 0.0010 "-" 1531 0`, time.UTC, `priority=134 facility_keyword=local0 level=info facility=16 severity=6 format=rfc5424 timestamp=2024-12-09T18:25:35.401631+00:00 hostname=ps999 app_name=account-server proc_id=- msg_id=- sd@51059.project=secret message="1.2.3.4 - - [09/Dec/2024:18:25:35 +0000] \"PUT someurl\" 201 - \"-\" \"-\" \"container-updater 1283500\" 0.0010 \"-\" 1531 0"`)
	f(`<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47 - This is a test message with structured data.`, time.UTC,
		`priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 message="This is a test message with structured data."`)
	f(`1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47 - This is a test message with structured data.`, time.UTC,
		`format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 message="This is a test message with structured data."`)
	f(`<165>1 2023-06-03T17:42:00Z mymachine.example.com appname 12345 ID47 [exampleSDID@32473 iut="3" eventSource="Application 123 = ] 56" eventID="11211"] This is a test message with structured data.`, time.UTC,
		`priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:00Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 exampleSDID@32473.iut=3 exampleSDID@32473.eventSource="Application 123 = ] 56" exampleSDID@32473.eventID=11211 message="This is a test message with structured data."`)
	f(`<165>1 2023-06-03T17:42:00Z mymachine.example.com appname 12345 ID47 [foo@123 iut="3"][bar@456 eventID="11211"][abc=def][x=y z=a q="]= "] This is a test message with structured data.`, time.UTC,
		`priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:00Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 foo@123.iut=3 bar@456.eventID=11211 abc=def x=y z=a q="]= " message="This is a test message with structured data."`)
	f(`<14>1 2025-02-11T12:31:28+01:00 synology Connection - - [synolog@6574 event_id="0x0001" synotype="Connection" username="synouser" luser="synouser" event="User [synouser\] from [192.168.0.10\] logged in successfully via [SSH\]." arg_1="synouser" arg_2="1027" arg_3="192.168.0.10" arg_4="SSH"][meta sequenceId="7"] User [synouser] from [192.168.0.10] logged in successfully via [SSH].`, time.UTC,
		`priority=14 facility_keyword=user level=info facility=1 severity=6 format=rfc5424 timestamp=2025-02-11T12:31:28+01:00 hostname=synology app_name=Connection proc_id=- msg_id=- synolog@6574.event_id=0x0001 synolog@6574.synotype=Connection synolog@6574.username=synouser synolog@6574.luser=synouser synolog@6574.event="User [synouser] from [192.168.0.10] logged in successfully via [SSH]." synolog@6574.arg_1=synouser synolog@6574.arg_2=1027 synolog@6574.arg_3=192.168.0.10 synolog@6574.arg_4=SSH meta.sequenceId=7 message="User [synouser] from [192.168.0.10] logged in successfully via [SSH]."`)
	f(`<14>1 2025-02-18T11:37:42+02:00 localhost Test - - [test event="quote \"test\""] Test message`, time.UTC, `priority=14 facility_keyword=user level=info facility=1 severity=6 format=rfc5424 timestamp=2025-02-18T11:37:42+02:00 hostname=localhost app_name=Test proc_id=- msg_id=- test.event="quote \"test\"" message="Test message"`)

	// Incomplete RFC 3164
	f("", time.UTC, ``)
	f("Jun  3 12:08:33", time.UTC, `format=rfc3164 timestamp=2024-06-03T12:08:33Z`)
	f("Foo  3 12:08:33", time.UTC, `format=rfc3164 message="Foo  3 12:08:33"`)
	f("Foo  3 12:08:33bar", time.UTC, `format=rfc3164 message="Foo  3 12:08:33bar"`)
	f("Jun  3 12:08:33 abcd", time.UTC, `format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd`)
	f("Jun  3 12:08:33 abcd sudo", time.UTC, `format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=sudo`)
	f("Jun  3 12:08:33 abcd sudo[123]", time.UTC, `format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=sudo proc_id=123`)
	f("Jun  3 12:08:33 abcd sudo foobar", time.UTC, `format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=sudo message=foobar`)
	f(`foo bar baz`, time.UTC, `format=rfc3164 message="foo bar baz"`)

	// Incomplete RFC 5424
	f(`<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47 [foo@123]`, time.UTC, `priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 foo@123=`)
	f(`<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47`, time.UTC, `priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47`)
	f(`<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345`, time.UTC, `priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345`)
	f(`<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname`, time.UTC, `priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname`)
	f(`<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com`, time.UTC, `priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com`)
	f(`<165>1 2023-06-03T17:42:32.123456789Z`, time.UTC, `priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z`)
	f(`<165>1 `, time.UTC, `priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424`)

	// RFC 3164 with RFC3339/ISO8601 timestamp (rsyslog RSYSLOG_ForwardFormat)
	f(`2025-01-23T12:15:23.965512+01:00 example rsyslogd: start`, time.UTC,
		`format=rfc3164 timestamp=2025-01-23T11:15:23.965512Z hostname=example app_name=rsyslogd message=start`)
	f(`<46>2025-01-23T12:15:23.965512+01:00 example rsyslogd: start`, time.UTC,
		`priority=46 facility_keyword=syslog level=info facility=5 severity=6 format=rfc3164 timestamp=2025-01-23T11:15:23.965512Z hostname=example app_name=rsyslogd message=start`)
	f(`2025-01-23T11:15:23Z example rsyslogd: start`, time.UTC,
		`format=rfc3164 timestamp=2025-01-23T11:15:23Z hostname=example app_name=rsyslogd message=start`)
	f(`<46>2025-01-23T11:15:23+00:00 example rsyslogd: start`, time.UTC,
		`priority=46 facility_keyword=syslog level=info facility=5 severity=6 format=rfc3164 timestamp=2025-01-23T11:15:23Z hostname=example app_name=rsyslogd message=start`)
	f(`2025-06-15T10:15:23-07:00 example rsyslogd: start`, time.UTC,
		`format=rfc3164 timestamp=2025-06-15T17:15:23Z hostname=example app_name=rsyslogd message=start`)
	f(`<46>2025-03-01T00:05:00+05:30 example rsyslogd: start`, time.UTC,
		`priority=46 facility_keyword=syslog level=info facility=5 severity=6 format=rfc3164 timestamp=2025-02-28T18:35:00Z hostname=example app_name=rsyslogd message=start`)
	f(`2025-08-12T09:00:00+07:00 example rsyslogd: start`, time.UTC,
		`format=rfc3164 timestamp=2025-08-12T02:00:00Z hostname=example app_name=rsyslogd message=start`)
	f(`2025-01-01T00:00:00.123+01:00 example rsyslogd: start`, time.UTC,
		`format=rfc3164 timestamp=2024-12-31T23:00:00.123Z hostname=example app_name=rsyslogd message=start`)
	f(`<46>2025-04-05T22:10:59.500000-04:00 example rsyslogd: start`, time.UTC,
		`priority=46 facility_keyword=syslog level=info facility=5 severity=6 format=rfc3164 timestamp=2025-04-06T02:10:59.5Z hostname=example app_name=rsyslogd message=start`)
	f(`2025-10-10T10:10:10.999000Z example rsyslogd: start`, time.UTC,
		`format=rfc3164 timestamp=2025-10-10T10:10:10.999Z hostname=example app_name=rsyslogd message=start`)

	// CEF - see https://www.microfocus.com/documentation/arcsight/arcsight-smartconnectors-8.3/cef-implementation-standard/Content/CEF/Chapter%201%20What%20is%20CEF.htm
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF cef.version=1 cef.device_vendor=Security cef.device_product=threatmanager cef.device_version=1.0 cef.device_event_class_id=100 cef.name="worm successfully stopped" cef.severity=10 cef.extension.src=10.0.0.1 cef.extension.dst=2.1.2.2 cef.extension.spt=1232`)
	f(`Sep 19 08:26:10 host CEF:0|Security|threatmanager|1.0|100|worm successfully\| \\stopped\n\r\=|10|s\=rc=10.0. \r\n\\\=  0.1  dst=2.1.2.2 spt=1232`, time.UTC, `format=rfc3164 timestamp=2024-09-19T08:26:10Z hostname=host app_name=CEF cef.version=0 cef.device_vendor=Security cef.device_product=threatmanager cef.device_version=1.0 cef.device_event_class_id=100 cef.name="worm successfully| \\stopped\n\r=" cef.severity=10 cef.extension.s=rc="10.0. \r\n\\=  0.1 " cef.extension.dst=2.1.2.2 cef.extension.spt=1232`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10|`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF cef.version=1 cef.device_vendor=Security cef.device_product=threatmanager cef.device_version=1.0 cef.device_event_class_id=100 cef.name="worm successfully stopped" cef.severity=10`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10|foobar=baz `, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF cef.version=1 cef.device_vendor=Security cef.device_product=threatmanager cef.device_version=1.0 cef.device_event_class_id=100 cef.name="worm successfully stopped" cef.severity=10 cef.extension.foobar="baz "`)
	f(`<6>Sep 14 14:12:51 10.x.x.143 CEF:0|FORCEPOINT|Firewall|6.8.6|70018|Connection_Allowed|0|deviceExternalId=NGFW1 node 1 dvchost=10.x.x.143 dvc=10.x.x.143 src=10.x.x.142 dst=20.x.x.209 spt=59358 dpt=443 proto=6 deviceInboundInterface=0 deviceOutboundInterface=1 act=Allow sourceTranslatedAddress=10.x.x.143 destinationTranslatedAddress=20.x.x.209 sourceTranslatedPort=27237 destinationTranslatedPort=443 deviceFacility=Packet Filtering rt=Sep 14 2021 14:12:51 app=HTTPS cs1Label=RuleID cs1=2100123.2 cs2Label=NatRuleId cs2=2099555.1`, time.UTC, `priority=6 facility_keyword=kern level=info facility=0 severity=6 format=rfc3164 timestamp=2024-09-14T14:12:51Z hostname=10.x.x.143 app_name=CEF cef.version=0 cef.device_vendor=FORCEPOINT cef.device_product=Firewall cef.device_version=6.8.6 cef.device_event_class_id=70018 cef.name=Connection_Allowed cef.severity=0 cef.extension.deviceExternalId="NGFW1 node 1" cef.extension.dvchost=10.x.x.143 cef.extension.dvc=10.x.x.143 cef.extension.src=10.x.x.142 cef.extension.dst=20.x.x.209 cef.extension.spt=59358 cef.extension.dpt=443 cef.extension.proto=6 cef.extension.deviceInboundInterface=0 cef.extension.deviceOutboundInterface=1 cef.extension.act=Allow cef.extension.sourceTranslatedAddress=10.x.x.143 cef.extension.destinationTranslatedAddress=20.x.x.209 cef.extension.sourceTranslatedPort=27237 cef.extension.destinationTranslatedPort=443 cef.extension.deviceFacility="Packet Filtering" cef.extension.rt="Sep 14 2021 14:12:51" cef.extension.app=HTTPS cef.extension.cs1Label=RuleID cef.extension.cs1=2100123.2 cef.extension.cs2Label=NatRuleId cef.extension.cs2=2099555.1`)
	f(`<6>1 2021-09-14T14:06:26-0500 10.x.x.147 - - - - CEF:0|FORCEPOINT|Firewall|6.10.0|76527|Sandbox_Unsupported-File-type|0|deviceExternalId=NGFW3 node 2 dvchost=10.x.x.147 dvc=10.x.x.147 deviceFacility=File Filtering rt=Sep 14 2021 14:06:26`, time.UTC, `priority=6 facility_keyword=kern level=info facility=0 severity=6 format=rfc5424 timestamp=2021-09-14T14:06:26-0500 hostname=10.x.x.147 app_name=- proc_id=- msg_id=- cef.version=0 cef.device_vendor=FORCEPOINT cef.device_product=Firewall cef.device_version=6.10.0 cef.device_event_class_id=76527 cef.name=Sandbox_Unsupported-File-type cef.severity=0 cef.extension.deviceExternalId="NGFW3 node 2" cef.extension.dvchost=10.x.x.147 cef.extension.dvc=10.x.x.147 cef.extension.deviceFacility="File Filtering" cef.extension.rt="Sep 14 2021 14:06:26"`)
	f(`<6>CEF:0|FORCEPOINT|Firewall|6.8.5|70019|Connection_Discarded|0|deviceExternalId=NGFW2 node 1 dvchost=10.x.x.149 dvc=10.x.x.149 src=10.x.x.4 dst=10.x.x.255 spt=138 dpt=138 proto=17 deviceInboundInterface=0 act=Discard msg=spoofed packet deviceFacility=Packet Filtering rt=Sep 14 2021 13:58:33 app=NetBIOS Datagram`, time.UTC, `priority=6 facility_keyword=kern level=info facility=0 severity=6 format=rfc3164 cef.version=0 cef.device_vendor=FORCEPOINT cef.device_product=Firewall cef.device_version=6.8.5 cef.device_event_class_id=70019 cef.name=Connection_Discarded cef.severity=0 cef.extension.deviceExternalId="NGFW2 node 1" cef.extension.dvchost=10.x.x.149 cef.extension.dvc=10.x.x.149 cef.extension.src=10.x.x.4 cef.extension.dst=10.x.x.255 cef.extension.spt=138 cef.extension.dpt=138 cef.extension.proto=17 cef.extension.deviceInboundInterface=0 cef.extension.act=Discard cef.extension.msg="spoofed packet" cef.extension.deviceFacility="Packet Filtering" cef.extension.rt="Sep 14 2021 13:58:33" cef.extension.app="NetBIOS Datagram"`)

	// Invalid CEF
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10|foobar`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message="1|Security|threatmanager|1.0|100|worm successfully stopped|10|foobar"`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message="1|Security|threatmanager|1.0|100|worm successfully stopped|10"`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message="1|Security|threatmanager|1.0|100|worm successfully stopped|"`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message="1|Security|threatmanager|1.0|100|worm successfully stopped"`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|1.0|100|`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|1.0|100`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|1.0|`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|1.0`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager|`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|`)
	f(`Sep 29 08:26:10 host CEF:1|Security|threatmanager`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager`)
	f(`Sep 29 08:26:10 host CEF:1|Security|`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|`)
	f(`Sep 29 08:26:10 host CEF:1|Security`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security`)
	f(`Sep 29 08:26:10 host CEF:1|`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|`)
	f(`Sep 29 08:26:10 host CEF:1`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1`)
	f(`Sep 29 08:26:10 host CEF:`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF`)
	f(`Sep 29 08:26:10 host CEF`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF`)
	f(`Sep 29 08:26:10 host`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host`)
	f(`Sep 29 08:26:10`, time.UTC, `format=rfc3164 timestamp=2024-09-29T08:26:10Z`)

	// @cee - https://cee.mitre.org/language/1.0-beta1/clt.html#syslog
	f(`Jun  3 12:08:33 abcd systemd[1]: @cee: {"k":"v","message":"test"}`, time.UTC, `format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=systemd proc_id=1 k=v message=test`)
	f(`Jun  3 12:08:33 abcd systemd[1]: @cee: {"k":"v","message":"two words"}`, time.UTC, `format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=systemd proc_id=1 k=v message="two words"`)
	f(`<0>Dec 20 12:42:20 syslog-relay process[35]: @cee: {"crit":123,"id":"abc","appname":"application","pname":"auth","pid":123,"host":"system.example.com","pri":10,"time":"2011-12-20T12:38:05.123456-05:00","action":"login","domain":"app","object":"account","service":"web","status":"success"}`, time.UTC, `priority=0 facility_keyword=kern level=emerg facility=0 severity=0 format=rfc3164 timestamp=2024-12-20T12:42:20Z hostname=syslog-relay app_name=process proc_id=35 crit=123 id=abc appname=application pname=auth pid=123 host=system.example.com pri=10 time=2011-12-20T12:38:05.123456-05:00 action=login domain=app object=account service=web status=success`)
	f(`<165>1 2011-12-20T12:38:06Z 10.10.0.1 process - example-event-1 @cee:{"pname":"auth","host":"system.example.com","time":"2011-12-20T12:38:05.123456-05:00"}`, time.UTC, `priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2011-12-20T12:38:06Z hostname=10.10.0.1 app_name=process proc_id=- msg_id=example-event-1 pname=auth host=system.example.com time=2011-12-20T12:38:05.123456-05:00`)
}
