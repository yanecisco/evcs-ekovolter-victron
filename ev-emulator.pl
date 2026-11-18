#!/usr/bin/perl
#
# evcs-emu.pl - emulace Victron EV Charging Station (EVCS) jako Modbus TCP slave.
#
# Venus OS / GX se tvári, že mluví s Victron EVCS (registry 5000-5143, unit 1,
# podle "EVCS-Modbus-TCP-register-list" v3.6-3.9), ve skutecnosti jsou data
# z jine nabijecky (fetch_charger_data) a zapisy z Venus OS jdou do charger_set.
#
# Pouziva jen core moduly perlu (perl-base / perl-modules-5.40 v Debianu 13):
# IO::Socket::INET, IO::Select, Socket, Errno, Time::HiRes.
#
# Spusteni:  ./evcs-emu.pl [-v]
#   EVCS_PORT=1502 ./evcs-emu.pl -v      (jiny port, napr. pro test bez roota)
#   EVCS_DEMO=1 ./evcs-emu.pl -v         (vzorova data misto fetch_charger_data)
#
# Port 502 vyzaduje root nebo CAP_NET_BIND_SERVICE (systemd: AmbientCapabilities).

use strict;
use warnings;
use Errno qw(EAGAIN EWOULDBLOCK);
use IO::Socket::INET;
use IO::Select;
use Socket qw(IPPROTO_TCP TCP_NODELAY);
use Time::HiRes qw(time);
use Digest::SHA qw(hmac_sha256_hex);
use LWP::UserAgent;
use Data::Dumper;
use JSON;
use HTTP::Tiny;

# ---------------------------------------------------------------- konfigurace
my $LISTEN_ADDR   = $ENV{EVCS_ADDR} // '0.0.0.0';
my $LISTEN_PORT   = $ENV{EVCS_PORT} // 1502;
my %UNITS         = (1 => 1);   # Modbus unit id, na ktera odpovidame
my $POLL_INTERVAL = 1;          # s, jak casto cteme skutecnou nabijecku
my $STALE_AFTER   = 60;         # s, po teto dobe bez uspesneho pollu odpovidame exception 0x0B
my $OVERRIDE_HOLD = 15;         # s, po zapisu drzime zapsanou hodnotu, nez ji potvrdi nova data
my $IDLE_TIMEOUT  = 300;        # s, zavreme necinneho klienta
my $PRODUCT_ID    = 0xC026;     # spec povoluje 0xC024 / 0xC025 / 0xC026
my $FW_H          = 0x0001;     # 5007: major
my $FW_L          = 0x0300;     # 5008: (minor << 8) | revision
my $SERIAL        = 'HQ0000EMUL01';   # presne 12 znaku
my $DEVICE_NAME   = 'EV Charger';     # 5027-5048, max 44 bajtu
my $VERBOSE       = 0;          # -v = radek se stavem nabijecky, -vv = navic Modbus provoz
for my $arg (@ARGV) {
    $VERBOSE += length($1) if $arg =~ /^-(v+)$/;      # -v = 1, -vv = 2
}
my $STATUS_EVERY  = 1;          # radek se stavem kazdy N-ty poll (1 = kazdych 5 s)

# InfluxDB 1.x (prazdne URL = vypnuto)
my $INFLUX_URL    = $ENV{INFLUX_URL};       # napr. 'http://127.0.0.1:8086'
my $INFLUX_DB     = $ENV{INFLUX_DB};
my $INFLUX_USER   = $ENV{INFLUX_USER};
my $INFLUX_PASS   = $ENV{INFLUX_PASS};
my $INFLUX_MEAS   = $ENV{INFLUX_MEAS};
my $INFLUX_BACKOFF = 30;        # s, pauza po neuspesnem zapisu (zapis blokuje max 2 s)

# Rozvrh pro Mode 2 (scheduled). dny: 1=po .. 7=ne; 'to' < 'from' = pres pulnoc.
# Prazdny seznam = mimo rozvrh porad, tedy nenabijime.
my @SCHEDULE = (
    # { days => [1 .. 7], from => '22:00', to => '06:00' },
);
my $GX_HOST = $ENV{GX_HOST} // '10.136.0.61';          # IP GX; prazdne = auto rezim bez dat ("waiting for sun")
my $GX_UNIT = 100;                          # com.victronenergy.system

my %AUTO = (
    start_soc      => 95,    # % SoC baterie, od kdy se smi zacit
    stop_soc       => 90,    # % SoC baterie, pod kterou se okamzite konci (hystereze k start_soc)
    min_a          => 6,     # A, minimalni proud (pod nim nenabijime)
    max_a          => 16,    # A, strop auto rezimu (dale omezeno maxCurrent z nabijecky)
    phases         => 3,     # fazi pro odhad pred startem (pri nabijeni se meri z proudu)
    volt           => 230,   # V, kdyz nabijecka nehlasi napeti
    start_margin_w => 200,   # W navic nad minimum pro start
    start_delay    => 90,    # s, prebytek musi vydrzet nad prahem, nez zacneme
    stop_delay     => 300,   # s, tolerance nizkeho prebytku (mrak, spotrebic) pri min. proudu
    min_on         => 180,   # s, nejkratsi beh pred zastavenim pro nizky prebytek
    min_off        => 120,   # s, nejkratsi pauza mezi koncem a dalsim startem
    ramp_up_delay  => 30,    # s, vyssi proud az kdyz vyssi prebytek vydrzel
    adj_interval   => 15,    # s, nejkratsi doba mezi zvysenimi proudu
    down_interval  => 10,    # s, nejkratsi doba mezi snizenimi proudu (krome ochrany menice)
    tol_w          => 150,   # W, mrtve pasmo pro snizovani proudu
    ema_tau        => 30,    # s, casova konstanta vyhlazeni prebytku
    no_draw_grace  => 180,   # s, auto neodebira (plne / nechce) -> konec relace
    retry_after    => 1800,  # s, pauza po "auto neodebira", nebo do odpojeni kabelu
    inv_limit_w    => 0,     # W, max. soucet zatizeni (dum + auto) pres menic; 0 = vypnuto
    ev_in_loads    => 1,     # 1 = AC consumption na GX uz obsahuje auto (odecte se)
    gx_stale       => 60,    # s, starsi data z GX = zastavit
    assist_soc      => 97,   # % SoC, od kdy smi start dotahnout chybejici vykon z baterie (0 = vypnuto)
    assist_keep_soc => 90,   # % SoC, pod kterou se dotahovani za behu vzda (hystereze)
    assist_w        => 1000, # W: o kolik smi byt prebytek pod minimem (vykon baterie / site)
);

my ($REG_FIRST, $REG_LAST) = (5000, 5143);

# -------------------------------------------- VASE FUNKCE (doplnit / nahradit)

# Vrati hashref s daty nabijecky ve formatu z dotazu, nebo undef pri chybe.
# Bool hodnoty mohou byt JSON::PP::Boolean i obycejne 0/1.
sub fetch_charger_data {
    if ($ENV{EVCS_DEMO}) {
        return {
            actualPower => 3680, chargedEnergy => 4.2, chargingCost => 0, chargingTime => 1830,
            currentL1 => 16, currentL2 => 0, currentL3 => 0,
            voltageL1 => 238, voltageL2 => '233.3000031', voltageL3 => '237.1000061',
            isVehicleConnected => 1, isCharging => 1, isChargingEnable => 1,
            isThreePhaseModeActive => 0, isThreePhaseModeAvailable => 1, isThreePhaseModeEnable => 0,
            maxCurrent => 16, targetCurrent => 16, kwhPrice => 0,
            totalChargedEnergy => '814.5599976',
            temperatures => { internal => '19.6', adapter => ['15.4', '15.2', '15.3'], relay => ['22.4', 24] },
        };
    }

        my $serial_number = $ENV{'EKOVOLTER_SN'};
        my $secret_key    = $ENV{'EKOVOLTER_SECRET'};
        my $base_uri      = $ENV{'EKOVOLTER_URI'};
        my $data = {};

        my $ua  = LWP::UserAgent->new(timeout => 10);
        foreach my $path ('/status', '/settings','/diagnostic') {
#       my $path = '/status';   # /status, /settings, /diagnostic, /type
                my $url  = "$base_uri/api/v1/charger$path";

                my $timestamp = int(time());          # sekundy
                my $body      = '';              # GET => prazdny string

                my $string_to_sign = "$url\n$timestamp\n$body";
                my $signature = hmac_sha256_hex($string_to_sign, $secret_key);

                my $req = HTTP::Request->new(GET => $url);
                $req->header('Authorization' => "HmacSHA256 $signature");
                $req->header('X-Timestamp'   => $timestamp);

                my $res = $ua->request($req);

                if ($res->is_success) {
                        my $data2 = decode_json($res->decoded_content);
                        foreach(keys %{$data2}) { 
                                $data->{$_} = $data2->{$_};
                        }
                } else {
                    dbg("wtf". $res->code);
                    return undef;
                }
        }
        #       print Dumper($data);

        return $data;
    

    # TODO: skutecne cteni dat z nabijecky
    #    return undef;
}

# Zapis nastaveni z Venus OS. $key je nazev pole v datech nabijecky, $value nova hodnota
# (bool pole dostanou 0/1). Pri chybe die() - bude zalogovano.
sub charger_set {
    my ($key, $value) = @_;
    # TODO: skutecny zapis do nabijecky
    logmsg("charger_set($key, $value)");
    if ($key eq 'isChargingEnable') {
        real_charger_set({isChargingEnable => $value ? JSON::true : JSON::false});
    }
    if ($key eq 'targetCurrent') {
        real_charger_set({targetCurrent => int($value)});
    }
    return 1;
}


sub real_charger_set {
        my $data = shift;
        my $serial_number = 'revcr01c00002838';
        my $secret_key    = 'revcr01c00002838';
        my $base_uri      = "http://10.136.0.177";

        my $path = '/settings';   # /status, /settings, /diagnostic, /type
        my $url  = "$base_uri/api/v1/charger$path";

        my $timestamp = int(time());          # sekundy
        my $body      = encode_json($data);

        my $string_to_sign = "$url\n$timestamp\n$body";
        my $signature = hmac_sha256_hex($string_to_sign, $secret_key);

        my $ua  = LWP::UserAgent->new(timeout => 10);
        my $req = HTTP::Request->new(PATCH => $url, undef, $body);
        $req->header('Authorization' => "HmacSHA256 $signature");
        $req->header('X-Timestamp'   => $timestamp);

        my $res = $ua->request($req);

        if ($res->is_success) {
                return 1;#decode_json($res->decoded_content);
        } else {
                dbg($res->code);
        }

        return undef;
};

# ------------------------------------------------------------ pomocne funkce
sub ts     { my @t = localtime; return sprintf('%02d:%02d:%02d', @t[2, 1, 0]) }
sub logmsg { print STDERR ts(), " @_\n" }
sub dbg    { logmsg(@_) if $VERBOSE >= 2 }

my ($data, $last_ok);

sub num {                      # ciselna hodnota z dat (0 kdyz chybi)
    my $v = $data ? $data->{$_[0]} : undef;
    return 0 if !defined $v || ref($v) =~ /^(?:ARRAY|HASH)$/;
    return $v + 0;                 # funguje i pro JSON::PP::Boolean
}
sub bool { num($_[0]) ? 1 : 0 }
sub u16 {
    my $x = sprintf('%.0f', $_[0]);
    return $x < 0 ? 0 : $x > 65535 ? 65535 : int($x);
}
sub amax { my $m = 0; for (@_) { $m = $_ if $_ > $m } $m }
sub is_stale { return !$last_ok || (time() - $last_ok > $STALE_AFTER) }

# ------------------------------------------------- tabulka zapisovatelnych reg
# addr default min max   (hodnoty raw, tj. vcetne skalovani; min<0 => int16)
my (%CFG, %LOCAL, %OVERRIDE, %REG);
while (my $l = <DATA>) {
    $l =~ s/#.*//;
    next unless $l =~ /^\s*(\d+)\s+(-?\d+)\s+(-?\d+)\s+(-?\d+)/;
    $CFG{$1}   = { min => $3, max => $4 };
    $LOCAL{$1} = $2 & 0xFFFF;
}
for my $a (5027 .. 5048) { $CFG{$a} = { min => 0, max => 65535 } }
@LOCAL{5027 .. 5048} = unpack('v22', pack('a44', $DEVICE_NAME));
$LOCAL{5109} = 1;

my %CMD = map { $_ => 1 } (5060, 5064);   # prikazove registry (kalibrace, reset) - ignorujeme

# registry, jejichz zapis predame do charger_set: addr => sub { (klic, hodnota) }
my %FORWARD = (
    5010 => sub { ('isChargingEnable',      $_[0] ? 1 : 0) },
    5016 => sub { ('targetCurrent',         $_[0]) },
    5017 => sub { ('maxCurrent',            $_[0]) },
    5055 => sub { ('isThreePhaseModeEnable', $_[0] == 0 ? 1 : 0) },   # 0 = 3f, 1 = 1f
);

# ------------------------------------------- mapovani dat nabijecky -> registry
sub charger_status {
    return 0 unless bool('isVehicleConnected');   # disconnected
    return 2 if bool('isCharging');               # charging
    my $mode = $LOCAL{5009} // 0;
    return auto_status() if $mode == 1;
    return 6 if $mode == 2 && !schedule_active(); # rozvrh: mimo okno
    return 6 unless bool('isChargingEnable');     # waiting for start
    return 3 if num('chargedEnergy') > 0;         # charged
    return 1;                                     # connected
}
# ------------------------------------------------------------------- rozvrh
sub schedule_active {
    my @t   = localtime;
    my $now = $t[2] * 60 + $t[1];
    my $wd  = $t[6] || 7;                          # 1=po .. 7=ne
    my $prev = $wd == 1 ? 7 : $wd - 1;
    for my $s (@SCHEDULE) {
        my %d = map { $_ => 1 } @{ $s->{days} };
        my ($f, $e) = map { my ($h, $m) = split /:/; $h * 60 + $m } @{$s}{qw(from to)};
        if ($f <= $e) {
            return 1 if $d{$wd} && $now >= $f && $now < $e;
        } else {                                   # pres pulnoc
            return 1 if $d{$wd}   && $now >= $f;
            return 1 if $d{$prev} && $now <  $e;
        }
    }
    return 0;
}

my $sched_last = 0;
sub schedule_control {                             # v Mode 2 ridi isChargingEnable podle rozvrhu
    return unless ($LOCAL{5009} // 0) == 2 && $data;
    my $want = schedule_active() ? 1 : 0;
    return if $want == bool('isChargingEnable') || time() - $sched_last < 30;
    $sched_last = time();
    logmsg("schedule: isChargingEnable -> $want");
    eval { charger_set('isChargingEnable', $want); 1 } or logmsg("charger_set failed: $@");
}
# ======================================================================
# AUTO rezim (Mode 1): nabijeni z prebytku slunce, data z GX (Modbus TCP, unit 100)
# ======================================================================

my ($gx_sock, $gx_tid, $gx_retry_at, $gx_fail) = (undef, 0, 0, 0);
my %GX;

sub s16 { $_[0] >= 0x8000 ? $_[0] - 0x10000 : $_[0] }
sub sum { my $s = 0; $s += $_ for @_; $s }

sub gx_recv {
    my ($n, $dl) = @_;
    my $buf = '';
    while (length($buf) < $n) {
        my $left = $dl - time();
        die "timeout\n" if $left <= 0 || !IO::Select->new($gx_sock)->can_read($left);
        my $got = sysread($gx_sock, my $c, $n - length($buf));
        die "connection closed\n" unless $got;
        $buf .= $c;
    }
    return $buf;
}

sub gx_read {                                   # FC03 na GX, vraci arrayref raw slov (die pri chybe)
    my ($addr, $qty) = @_;
    my $w = eval {
        $gx_sock ||= IO::Socket::INET->new(PeerAddr => $GX_HOST, PeerPort => 502, Proto => 'tcp', Timeout => 2)
            or die "connect: $!\n";
        my $tid = ++$gx_tid & 0xFFFF;
        my $dl  = time() + 2;
        defined syswrite($gx_sock, pack('nnnCCnn', $tid, 0, 6, $GX_UNIT, 3, $addr, $qty)) or die "write: $!\n";
        my ($rt, $rp, $rl) = unpack('nnn', gx_recv(7, $dl));
        die "bad header\n" if $rt != $tid || $rp != 0 || $rl < 3 || $rl > 260;
        my $pdu = gx_recv($rl - 1, $dl);
        my ($fc, $bc) = unpack('CC', $pdu);
        die "exception $bc\n" if $fc & 0x80;
        die "bad reply\n" if $fc != 3 || $bc != $qty * 2 || length($pdu) != 2 + $bc;
        [ unpack('n*', substr($pdu, 2)) ];
    };
    unless ($w) {
        my $e = $@ || "unknown error\n";
        $gx_sock->close if $gx_sock;
        $gx_sock = undef;
        die $e;
    }
    return $w;
}

sub gx_poll {
    my ($now) = @_;
    return 0 unless $GX_HOST && $now >= $gx_retry_at;
    my $ok = eval {
        my $a = gx_read(808, 15);               # 808-816 PV AC (vystup/vstup/genset), 817-819 spotreba, 820-822 sit
        my $b = gx_read(842, 3);                # vykon baterie, SoC, stav
        my $c = gx_read(850, 1);                # PV DC-coupled
        %GX = (
            pv    => sum(@$a[0 .. 8]) + $c->[0],
            loads => sum(@$a[9 .. 11]),
            grid  => sum(map { s16($_) } @$a[12 .. 14]),
            batt  => s16($b->[0]),
            soc   => $b->[1],
            ts    => $now,
        );
        1;
    };
    if ($ok) {
        logmsg('GX: recovered') if $gx_fail;
        $gx_fail = 0;
        return 1;
    }
    (my $e = $@) =~ s/\s+$//;
    logmsg("GX read failed: $e") unless $gx_fail++;
    $gx_retry_at = $now + 10;                   # zapis/cteni blokuje max 2 s, pri vypadku zkousime po 10 s
    return 0;
}

my %AS = (last_stop => 0, retry_at => 0, last_cmd => 0);
sub auto_reset {
    my %keep = map { $_ => $AS{$_} } qw(last_stop retry_at last_cmd);
    %AS = (state => 'off', cur => 0, excess => 0, ema => 0, %keep);
}
auto_reset();

sub auto_cmd {                                   # charger_set s logem, vraci 1 = ok
    my ($now, @call) = @_;
    $AS{last_cmd} = $now;
    return 1 if eval { charger_set(@call); 1 };
    (my $e = $@) =~ s/\s+$//;
    logmsg("auto: charger_set(@call) failed: $e");
    return 0;
}

sub auto_start {
    my ($now, $a) = @_;
    logmsg("auto: START at $a A (ema $AS{ema} W)");
    auto_cmd($now, 'targetCurrent', $a) && auto_cmd($now, 'isChargingEnable', 1)
        or do { $AS{retry_at} = $now + 60; return };
    delete @AS{qw(ok_since low_since up_since)};
    @AS{qw(state cur started last_adj done_since)} = ('on', $a, $now, $now, $now);
}

sub auto_stop {
    my ($now, $why) = @_;
    logmsg("auto: STOP ($why)");
    auto_cmd($now, 'isChargingEnable', 0);
    delete @AS{qw(ok_since low_since up_since done_since)};
    @AS{qw(state cur last_stop)} = ('off', 0, $now);
}

sub auto_set_current {
    my ($now, $a) = @_;
    return if $a == $AS{cur};
    logmsg("auto: current $AS{cur} -> $a A (ema $AS{ema} W)");
    return unless auto_cmd($now, 'targetCurrent', $a);
    $AS{cur}      = $a;
    $AS{last_adj} = $now;
}

sub auto_control {
    my $now = shift // time();
    unless (($LOCAL{5009} // 0) == 1 && $GX_HOST) { auto_reset() if $AS{state} ne 'off' || defined $AS{last_t}; return }
    return unless $data;
    gx_poll($now);

    my $connected = bool('isVehicleConnected');
    if (!$connected) {
        if ($AS{state} eq 'on') { $AS{state} = 'off'; $AS{last_stop} = $now; $AS{cur} = 0 }
        $AS{retry_at} = 0;
        delete @AS{qw(ok_since low_since up_since done_since)};
    }
    # mimo nase rizeni nesmi zustat nabijecka povolena (jinak by auto po zasunuti startovalo samo)
    if ($AS{state} eq 'off' && bool('isChargingEnable') && $now - $AS{last_cmd} >= 30) {
        logmsg('auto: charger enabled outside auto control -> disable');
        auto_cmd($now, 'isChargingEnable', 0);
    }

    my $on = $AS{state} eq 'on';
    unless ($GX{ts} && $now - $GX{ts} <= $AUTO{gx_stale}) {
        auto_stop($now, 'GX data stale') if $on;
        return;
    }
    return unless $connected;

    my $dt = defined $AS{last_t} ? $now - $AS{last_t} : 0;
    $AS{last_t} = $now;

    # --- vstupy
    my @v = grep { $_ > 100 } map { num("voltageL$_") } 1 .. 3;
    my $V   = @v ? sum(@v) / @v : $AUTO{volt};
    my $ph  = $on ? scalar(grep { num("currentL$_") > 1 } 1 .. 3) : 0;
    $ph ||= $AUTO{phases};
    my $vph = $V * $ph;
    my $min_a = $AUTO{min_a};
    my $min_w = $min_a * $vph;
    my $max_a = $AUTO{max_a};
    my $mc    = num('maxCurrent');
    $max_a = $mc if $mc >= $min_a && $mc < $max_a;

    my $ev   = $on ? num('actualPower') : 0;
    my $base = $GX{loads} - ($AUTO{ev_in_loads} ? $ev : 0);        # zatizeni bez auta
    $base = 0 if $base < 0;
    my $raw  = $GX{pv} - $base;                                    # prebytek slunce
    my $room = $AUTO{inv_limit_w} ? $AUTO{inv_limit_w} - $base : undef;
    $raw = $room if defined $room && $room < $raw;

    if (!defined $AS{ema_init}) { $AS{ema} = $raw; $AS{ema_init} = 1 }
    else { $AS{ema} += ($raw - $AS{ema}) * ($dt > 0 ? 1 - exp(-$dt / $AUTO{ema_tau}) : 0) }
    $AS{excess} = $raw;
    my $ema = $AS{ema};
    my $soc = $GX{soc};

    # --- vypnuto: cekame na slunce
    my $assist = 0;
    if ($AUTO{assist_soc}) {
        my $lim = $on ? $AUTO{assist_keep_soc} : $AUTO{assist_soc};
        $assist = $AUTO{assist_w} if $soc >= $lim;
    }    
    if (!$on) {
        $AS{lowsoc} = $soc < $AUTO{start_soc} ? 1 : 0;
        my $ready = !$AS{lowsoc}
                 && $ema >= $min_w + $AUTO{start_margin_w} - $assist
                 && $now - $AS{last_stop} >= $AUTO{min_off}
                 && $now >= $AS{retry_at};
        if ($ready) {
            $AS{ok_since} //= $now;
            auto_start($now, $min_a) if $now - $AS{ok_since} >= $AUTO{start_delay};
        } else {
            delete $AS{ok_since};
        }
        return;
    }

    # --- zapnuto
    return auto_stop($now, "SoC $soc% < $AUTO{stop_soc}%") if $soc < $AUTO{stop_soc};

    my $cap;                                                       # ochrana menice: okamzite, bez prodlev
    if (defined $room) {
        $cap = int($room / $vph);
        return auto_stop($now, 'inverter limit') if $cap < $min_a;
        auto_set_current($now, $cap) if $AS{cur} > $cap;
    }

    if (bool('isCharging') || num('actualPower') > 200) { delete $AS{done_since} }
    else {
        $AS{done_since} //= $now;
        if ($now - $AS{done_since} >= $AUTO{no_draw_grace}) {
            $AS{retry_at} = $now + $AUTO{retry_after};
            return auto_stop($now, 'vehicle not drawing power');
        }
    }

    if ($ema < $min_w - $assist) {                                 # mrak / velky spotrebic: drzime minimum
        $AS{low_since} //= $now;
        delete $AS{up_since};
        auto_set_current($now, $min_a);
        return auto_stop($now, 'low excess')
            if $now - $AS{low_since} >= $AUTO{stop_delay} && $now - $AS{started} >= $AUTO{min_on};
        return;
    }
    delete $AS{low_since};

    my $want = int($ema / $vph);
    $want = $max_a if $want > $max_a;
    $want = $min_a if $want < $min_a;
    $want = $cap if defined $cap && $want > $cap;
    my $cur = $AS{cur};
    if ($want < $cur) {
        auto_set_current($now, $want)
            if $ema < $cur * $vph - $AUTO{tol_w} && $now - $AS{last_adj} >= $AUTO{down_interval};
        delete $AS{up_since};
    }
    elsif ($want > $cur) {
        $AS{up_since} //= $now;
        if ($now - $AS{up_since} >= $AUTO{ramp_up_delay} && $now - $AS{last_adj} >= $AUTO{adj_interval}) {
            auto_set_current($now, $cur + ($want - $cur > 2 ? 2 : $want - $cur));
            delete $AS{up_since};
        }
    }
    else { delete $AS{up_since} }
}

sub auto_status {                                # Victron status pro Mode 1
    return 4 unless $GX_HOST && $GX{ts};
    return 21 if $AS{state} eq 'on';             # start charging
    return 7 if $AS{lowsoc};                     # low SoC
    return 4;                                    # waiting for sun
}

sub auto_info {                                  # kratky text pro radek -v
    return 'AUTO bez GX_HOST' unless $GX_HOST;
    return 'AUTO ceka na GX' unless $GX{ts};
    my $now = time();
    my $t = '';
    if ($AS{state} eq 'on') { $t = sprintf(' stop za %ds', $AUTO{stop_delay} - ($now - $AS{low_since})) if $AS{low_since} }
    elsif ($AS{ok_since})   { $t = sprintf(' start za %ds', $AUTO{start_delay} - ($now - $AS{ok_since})) }
    return sprintf('AUTO %s PV%d load%d grid%+d bat%+d soc%d%%%s exc%+d avg%+dW %dA%s', $AS{state},
      @GX{qw(pv loads grid batt)}, $GX{soc},
      ($AS{lowsoc} && $AS{state} eq 'off' ? "<$AUTO{start_soc}" : ''),
      $AS{excess}, $AS{ema}, $AS{cur}, $t);
}

# ------------------------------------------------------- vypis a InfluxDB
my @ST = qw(DISC CONN CHG DONE SUN RFID WAIT LOWSOC);

sub temps {                                        # (internal, max adapter, max relay)
    my $t = ref($data->{temperatures}) eq 'HASH' ? $data->{temperatures} : {};
    return (($t->{internal} // 0) + 0,
            amax(map { $_ + 0 } @{ ref($t->{adapter}) eq 'ARRAY' ? $t->{adapter} : [] }),
            amax(map { $_ + 0 } @{ ref($t->{relay})   eq 'ARRAY' ? $t->{relay}   : [] }));
}

sub status_line {
    my $st = charger_status();
    my @I  = map { num("currentL$_") } 1 .. 3;
    my @V  = map { num("voltageL$_") } 1 .. 3;
    my $s  = int(num('chargingTime'));
    my @tm = localtime;
    my $line = sprintf('%-6s %5dW %4.1f/%4.1f/%4.1fA %3d/%3d/%3dV %5.2fkWh %d:%02d tot %.1f | M%d en%d set%d/%dA %dph | T%.0f/%.0f/%.0f',
        $ST[$st] // $st, num('actualPower')*1000, @I, @V,    
        num('chargedEnergy'), int($s / 3600), int($s % 3600 / 60), num('totalChargedEnergy'),
        $LOCAL{5009} // 0, bool('isChargingEnable'), num('targetCurrent'), num('maxCurrent'),
        (bool('isThreePhaseModeActive') ? 3 : 1), temps());
    $line .= ' | ' . auto_info() if ($LOCAL{5009} // 0) == 1;
    return $line;  
}

my ($http, $influx_retry_at, $influx_fail) = (undef, 0, 0);
sub influx_write {
    return unless $INFLUX_URL && $data;
    return if time() < $influx_retry_at;
    $http ||= HTTP::Tiny->new(timeout => 2, agent => 'evcs-emu');

    my ($ti, $ta, $tr) = temps();
    my %f = (
        status => charger_status(), mode => $LOCAL{5009} // 0,
        power => num('actualPower')*1000,
        (map { ("current_l$_" => num("currentL$_"), "voltage_l$_" => num("voltageL$_")) } 1 .. 3),
        session_kwh => num('chargedEnergy'), session_s => num('chargingTime'),
        total_kwh => num('totalChargedEnergy'), cost => num('chargingCost'),
        target_a => num('targetCurrent'), max_a => num('maxCurrent'),
        connected => bool('isVehicleConnected'), charging => bool('isCharging'),
        enabled => bool('isChargingEnable'), phases3_active => bool('isThreePhaseModeActive'),
        temp_internal => $ti, temp_adapter_max => $ta, temp_relay_max => $tr,
        (($LOCAL{5009} // 0) == 1 && $GX{ts})
        ? (gx_pv => $GX{pv}, gx_loads => $GX{loads}, gx_grid => $GX{grid}, gx_batt => $GX{batt},
          gx_soc => $GX{soc}, auto_excess => $AS{excess}, auto_ema => $AS{ema},
          auto_on => ($AS{state} eq 'on' ? 1 : 0))
        : (),
    );
    (my $tag = $SERIAL) =~ s/([ ,=])/\\$1/g;
    my $line = "$INFLUX_MEAS,charger=$tag "
             . join(',', map { "$_=" . ($f{$_} + 0) } sort keys %f)   # bez 'i' = vzdy float
             . ' ' . sprintf('%.0f', time() * 1e9);

    my $esc = sub { (my $s = shift) =~ s/([^A-Za-z0-9_.~-])/sprintf('%%%02X', ord $1)/ge; $s };
    my $url = "$INFLUX_URL/write?precision=ns&db=" . $esc->($INFLUX_DB);
    $url .= '&u=' . $esc->($INFLUX_USER) . '&p=' . $esc->($INFLUX_PASS) if length $INFLUX_USER;

    my $r = $http->post($url, { content => $line, headers => { 'Content-Type' => 'text/plain' } });
    if ($r->{success}) {
        logmsg('influx: recovered') if $influx_fail;
        $influx_fail = 0;
    } else {
        logmsg("influx: write failed ($r->{status} " . substr($r->{content} // $r->{reason} // '', 0, 120) . ')') unless $influx_fail++;
        $influx_retry_at = time() + $INFLUX_BACKOFF;
    }
}

sub rebuild_regs {
    my %r = %LOCAL;
    $r{5000} = $PRODUCT_ID;
    @r{5001 .. 5006} = unpack('v6', pack('a12', $SERIAL));
    $r{5007} = $FW_H;
    $r{5008} = $FW_L;

    if ($data) {
        my @I = map { num("currentL$_") }  1 .. 3;
        my @V = map { num("voltageL$_") }  1 .. 3;
        my $st = charger_status();

        $r{5010} = bool('isChargingEnable');
        $r{5011 + $_} = u16($I[$_] * $V[$_]) for 0 .. 2;        # W, V*A (cos fi neznáme)
        $r{5014} = u16(num('actualPower')*1000);                     # kW
        $r{5015} = $st;
        $r{5016} = u16(num('targetCurrent'));
        $r{5017} = u16(num('maxCurrent'));
        $r{5018} = u16(amax(@I) * 10);                          # A*10

        my $t = int(num('chargingTime'));                       # predpoklad: s
        $r{5019} = ($t >> 16) & 0xFFFF;
        $r{5020} = $t & 0xFFFF;
        $r{5021} = u16(num('chargedEnergy') * 100);             # predpoklad: kWh
        $r{5022} = u16(num('chargingCost') * 100);
        my $te = int(num('totalChargedEnergy') * 100 + 0.5);    # (H*65536+L)/100 = kWh
        $r{5023} = ($te >> 16) & 0xFFFF;
        $r{5024} = $te & 0xFFFF;
        $r{5025} = $st == 2 ? 600 : $st == 0 ? 1200 : 900;      # syntetické CP napeti (V*100)

        $r{5055} = bool('isThreePhaseModeEnable') ? 0 : 1;
        $r{5100} = bool('isThreePhaseModeAvailable');
        $r{5109} = bool('isThreePhaseModeActive') ? 3 : 1;
        $r{5117} = bool('isCharging') ? (bool('isThreePhaseModeActive') ? 0b1111 : 0b1001) : 0;

        my $tmp = ref($data->{temperatures}) eq 'HASH' ? $data->{temperatures} : {};
        my $int = ($tmp->{internal} // 0) + 0;
        my $adp = amax(map { $_ + 0 } @{ ref($tmp->{adapter}) eq 'ARRAY' ? $tmp->{adapter} : [] });
        my $rel = amax(map { $_ + 0 } @{ ref($tmp->{relay})   eq 'ARRAY' ? $tmp->{relay}   : [] });
        $r{5122} = u16($int * 10);     # onboard
        $r{5123} = u16($int * 10);     # display
        $r{5124} = u16($adp * 10);     # car connector
        $r{5125} = u16($int * 10);     # power meter
        $r{5126} = u16($rel * 10);     # power control
        $r{5127} = u16($int * 10);     # power supply

        $r{5141} = u16(num('chargingCost') * 100);
        my $pr = int(num('kwhPrice') * 1000 + 0.5);             # (H*65536+L)/1000
        $r{5142} = ($pr >> 16) & 0xFFFF;
        $r{5143} = $pr & 0xFFFF;
    }

    my $now = time();
    for my $a (keys %OVERRIDE) {
        if ($OVERRIDE{$a}[1] < $now) { delete $OVERRIDE{$a}; next }
        $r{$a} = $OVERRIDE{$a}[0];
    }
    $r{5111} = (int($now - $^T) >> 16) & 0xFFFF;                # uptime
    $r{5112} = int($now - $^T) & 0xFFFF;

    %REG = %r;
}
my $poll_count = 0;
sub poll_charger {
    my $d = eval { fetch_charger_data() };
    if (!$d || ref($d) ne 'HASH') {
        logmsg('poll: no data' . ($@ ? ": $@" : ''));
        return;
    }
    $data    = $d;
    $last_ok = time();
    rebuild_regs();
    schedule_control();
    print STDERR ts(), ' ', status_line(), "\n" if $VERBOSE >= 1 && ($poll_count++ % $STATUS_EVERY) == 0;    
    influx_write();    
}

# -------------------------------------------------------------- zapis registru
sub validate_write {            # vraci 0 = ok, jinak modbus exception code
    my ($a, $w) = @_;
    my $c = $CFG{$a} or return 2;
    my $v = ($c->{min} < 0 && $w >= 0x8000) ? $w - 0x10000 : $w;
    return 3 if $v < $c->{min} || $v > $c->{max};
    return 0;
}

sub apply_write {
    my ($a, $w) = @_;
    if ($CMD{$a}) { logmsg("write cmd reg $a = $w (ignored)"); return }
    $LOCAL{$a}    = $w;
    $OVERRIDE{$a} = [ $w, time() + $OVERRIDE_HOLD ];
    rebuild_regs();
    my $v = ($CFG{$a}{min} < 0 && $w >= 0x8000) ? $w - 0x10000 : $w;
    if ($a >= 5027 && $a <= 5048) {
        if ($a == 5048) {       # nazev zapsan cely, dekodujeme az pri poslednim slove
            (my $name = pack('v22', @LOCAL{5027 .. 5048})) =~ s/\0.*//s;
            logmsg("write device name = '$name'");
        }
        return;
    }
    logmsg("write reg $a = $v");

    my @call;
    # 5010/5016 (start/stop, proud) predavame nabijecce jen v manualu; v auto/rozvrhu rizeni drzime my
    if ($FORWARD{$a} && !(($a == 5010 || $a == 5016) && ($LOCAL{5009} // 0) != 0)) {    
        @call = $FORWARD{$a}->($v);
    }
    elsif ($a == 5143) {        # cena: H se zapisuje prvni, predame az pri zapisu L
        @call = ('kwhPrice', ((($REG{5142} // 0) * 65536) + $w) / 1000);
    }
    if (@call) {
        eval { charger_set(@call); 1 } or logmsg("charger_set(@call) failed: $@");
    }
}

# ---------------------------------------------------------------- Modbus PDU
sub exception { my ($fc, $code) = @_; return pack('CC', $fc | 0x80, $code) }

sub handle_request {
    my ($uid, $fc, $body) = @_;
    return exception($fc, 0x0B) unless $UNITS{$uid};

    if ($fc == 3 || $fc == 4) {
        return exception($fc, 3) if length($body) != 4;
        my ($addr, $qty) = unpack('nn', $body);
        return exception($fc, 3) if $qty < 1 || $qty > 125;
        return exception($fc, 2) if $addr < $REG_FIRST || $addr + $qty - 1 > $REG_LAST;
        return exception($fc, 0x0B) if is_stale();
        dbg("  modbus read $addr x $qty");        
        return pack('CC', $fc, $qty * 2) . pack('n*', map { $REG{$_} // 0 } $addr .. $addr + $qty - 1);
    }
    if ($fc == 6) {
        return exception($fc, 3) if length($body) != 4;
        my ($addr, $val) = unpack('nn', $body);
        return exception($fc, 0x0B) if is_stale();
        if (my $e = validate_write($addr, $val)) { return exception($fc, $e) }
        apply_write($addr, $val);
        return pack('Cnn', $fc, $addr, $val);
    }
    if ($fc == 16) {
        return exception($fc, 3) if length($body) < 6;
        my ($addr, $qty, $bc) = unpack('nnC', $body);
        return exception($fc, 3) if $qty < 1 || $qty > 123 || $bc != $qty * 2 || length($body) != 5 + $bc;
        return exception($fc, 0x0B) if is_stale();
        my @w = unpack('n*', substr($body, 5));
        for my $i (0 .. $#w) {
            if (my $e = validate_write($addr + $i, $w[$i])) { return exception($fc, $e) }
        }
        apply_write($addr + $_, $w[$_]) for 0 .. $#w;
        return pack('Cnn', $fc, $addr, $qty);
    }
    return exception($fc, 1);   # illegal function
}

# ------------------------------------------------------------------ sit
$SIG{PIPE} = 'IGNORE';

my $listen = IO::Socket::INET->new(
    LocalAddr => $LISTEN_ADDR, LocalPort => $LISTEN_PORT,
    Proto => 'tcp', Listen => 10, ReuseAddr => 1,
) or die "listen $LISTEN_ADDR:$LISTEN_PORT: $!\n";
$listen->blocking(0);
my $sel = IO::Select->new($listen);
my %clients;
logmsg("EVCS emulator listening on $LISTEN_ADDR:$LISTEN_PORT");

sub drop_client {
    my ($cl, $why) = @_;
    dbg("  client $cl->{peer} closed ($why)");    
    $sel->remove($cl->{sock});
    delete $clients{ fileno $cl->{sock} };
    close $cl->{sock};
}

sub send_all {
    my ($sock, $buf) = @_;
    my ($off, $tries) = (0, 0);
    while ($off < length $buf) {
        my $w = syswrite($sock, $buf, length($buf) - $off, $off);
        if (!defined $w) {
            return 0 if $! != EAGAIN && $! != EWOULDBLOCK;
            return 0 if ++$tries > 200;
            select(undef, undef, undef, 0.01);
            next;
        }
        $off += $w;
    }
    return 1;
}

sub process_buffer {
    my ($cl) = @_;
    while (length($cl->{buf}) >= 6) {
        my ($tid, $pid, $len) = unpack('nnn', $cl->{buf});
        return 0 if $pid != 0 || $len < 2 || $len > 254;          # neplatny MBAP
        my $total = 6 + $len;
        last if length($cl->{buf}) < $total;
        my $frame = substr($cl->{buf}, 0, $total, '');
        my ($uid, $fc) = unpack('x6 C C', $frame);
        my $pdu  = handle_request($uid, $fc, substr($frame, 8));
        my $resp = pack('nnnC', $tid, 0, length($pdu) + 1, $uid) . $pdu;
        return 0 unless send_all($cl->{sock}, $resp);
    }
    return 1;
}

my $next_poll = 0;
while (1) {
    my $wait = $next_poll - time();
    $wait = 0 if $wait < 0;
    $wait = 1 if $wait > 1;

    for my $fh ($sel->can_read($wait)) {
        if (fileno($fh) == fileno($listen)) {
            my $c = $listen->accept or next;
            $c->blocking(0);
            setsockopt($c, IPPROTO_TCP, TCP_NODELAY, 1);
            $sel->add($c);
            $clients{ fileno $c } = { sock => $c, buf => '', last => time(), peer => ($c->peerhost // '?') . ':' . ($c->peerport // '?') };
            dbg("  client $clients{fileno $c}{peer} connected");
            next;
        }
        my $cl = $clients{ fileno $fh } or next;
        my $n = sysread($fh, my $chunk, 4096);
        if (!defined $n) {
            next if $! == EAGAIN || $! == EWOULDBLOCK;
            drop_client($cl, "read error: $!");
        }
        elsif ($n == 0) {
            drop_client($cl, 'eof');
        }
        else {
            $cl->{buf} .= $chunk;
            $cl->{last} = time();
            drop_client($cl, 'protocol/write error') unless process_buffer($cl);
        }
    }

    my $now = time();
    if ($now >= $next_poll) {
        poll_charger();
        auto_control();
        $next_poll = $now + $POLL_INTERVAL;
        $next_poll = time() if $next_poll < time();
    }
    for my $cl (values %clients) {
        drop_client($cl, 'idle timeout') if time() - $cl->{last} > $IDLE_TIMEOUT;
    }
}

__DATA__
# addr default min max     (EVCS-Modbus-TCP-register-list, raw hodnoty)
5009 0 0 2
5010 0 0 1
5016 6 6 32
5017 10 6 32
5026 0 0 1
5049 1 0 1
5050 1 0 1
5051 100 1 100
5052 7 1 100
5053 120 1 65535
5054 0 0 1
5055 1 0 1
5056 1 0 2
5057 4800 1 65535
5058 4300 1 65535
5059 60 1 65535
5060 0 1 4
5062 6 6 31
5063 100 60 140
5064 0 0 1
5065 1 0 1
5066 90 1 100
5067 20 1 100
5068 0 0 1
5069 30 1 65535
5070 60 1 65535
5071 -500 -32768 32767
5072 -1400 -32768 32767
5073 -250 -32768 32767
5074 250 -32768 32767
5075 -100 -32768 32767
5076 2 1 65535
5077 1000 1 65535
5078 30 1 65535
5079 60 1 65535
5080 10 1 65535
5081 500 0 65535
5082 80 1 100
5083 100 0 65535
5084 25 0 65535
5085 400 1 65535
5086 400 1 65535
5087 7 1 65535
5088 45 1 65535
5089 15 1 65535
5090 10 1 65535
5091 -1500 -32768 -1
5092 300 1 65535
5093 0 0 65535
5099 0 0 1
5100 0 0 1
5101 10 1 65535
5102 0 0 1
5103 30 5 65535
5104 0 0 1
5105 135 1 65535
5106 10 1 65535
5107 5 1 65535
5108 2000 0 65535
5110 600 0 65535
5118 5 1 65535
5119 1 1 65535
5120 20 1 65535
5128 0 0 1
5129 0 0 65535
5130 1 0 1
5131 60 0 65535
5132 100 0 100
5133 1 0 1
5134 500 1 65535
5135 0 -32768 32767
5136 100 0 100
5137 0 0 1
5138 99 1 100
5139 10 1 65535
5142 0 0 65535
5143 130 0 65535
