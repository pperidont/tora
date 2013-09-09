/* ************************************************************************ */
/*																			*/
/*  Tora - Neko Application Server											*/
/*  Copyright (c)2013 Motion-Twin											*/
/*																			*/
/* This library is free software; you can redistribute it and/or			*/
/* modify it under the terms of the GNU Lesser General Public				*/
/* License as published by the Free Software Foundation; either				*/
/* version 2.1 of the License, or (at your option) any later version.		*/
/*																			*/
/* This library is distributed in the hope that it will be useful,			*/
/* but WITHOUT ANY WARRANTY; without even the implied warranty of			*/
/* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU		*/
/* Lesser General Public License or the LICENSE file for more details.		*/
/*																			*/
/* ************************************************************************ */

class RedisManager {
	
	public var key : String;
	public var host : String;
	public var port : Int;

	public var queues : Map<String, ModToraApi.Queue>;
	public var lock : neko.vm.Mutex;

	// Subscribe
	var cnx : redis.Connection;
	public var buffer : neko.vm.Deque<Array<String>>;
	public var thread : neko.vm.Thread;
	var connected : Bool;

	// Publish
	var pcnx : redis.Connection;
	public var pbuffer : neko.vm.Deque<Array<String>>;
	public var pthread : neko.vm.Thread;

	public var closing : Bool;

	public function new( key : String, host : String, port : Int ){
		this.lock = new neko.vm.Mutex();
		this.key = key;		
		this.host = host;
		this.port = port;

		queues = new Map();
		closing = false;
		connected = false;

		buffer = new neko.vm.Deque();
		thread = neko.vm.Thread.create(subscribeLoop);

		pbuffer = new neko.vm.Deque();
		pthread = neko.vm.Thread.create(publishLoop);
	}

	function subscribeLoop(){
		try {
			var err = false;

			try {
				cnx = new redis.Connection(host,port);
				cnx.setTimeout(1);
				connected = true;
				Tora.log("[Redis-Subscribe] Connected to "+key+".");
			}catch(e : Dynamic ){
			}

			while( true ){
				if( closing )
					break;
				var resp = null;
				try {
					if( cnx == null )
						throw "Eof";
					while( true ){
						var a = buffer.pop(false);
						if( a == null )
							break;
						cnx.command(a.shift(),a);
					}
					resp = cnx.receive();
				}catch( e : Dynamic ){
					switch( Std.string(e) ){
						case "Blocked":
						case "Eof","{}": // TODO why Eof become "{}" ??
							try {
								if( !err ){
									err = true;
									connected = false;
									Tora.log("[Redis-Subscribe] "+key+" connection lost. Try to reconnect...");
								}
								cnx = new redis.Connection(host,port);
								cnx.setTimeout(1);
								var a = new Array();
								lock.acquire();
								for( q in queues )
									a.push(q.name);
								while( true ){
									var a = buffer.pop(false);
									if( a == null )
										break;
								}
								lock.release();
								cnx.command("SUBSCRIBE",a);
								Tora.log("[Redis-Subscribe] "+key+" re-connected OK.");
								connected = true;
								err = false;
							}catch( e : Dynamic ){
								Sys.sleep(1);
							}
						default:
							neko.Lib.rethrow(e);
					}
				}
				if( resp != null ){
					switch( resp ){
						case R_Multi(a):
							if( redis.Connection.str(a[0]) == "message" )
								onMessage(redis.Connection.str(a[1]),redis.Connection.str(a[2]));
						default: //
					}
				}
			}

			if( cnx != null ){
				try{
					cnx.close();
				}catch( e : Dynamic ){
				}
				cnx = null;
				buffer = null;
				Tora.log("[Redis-Subscribe] "+key+" connection closed.");
			}
		}catch(e : Dynamic ){
			Sys.print("Unrecoverable error in redis subscribeLoop: ");
			Sys.println( e );
			Sys.println( haxe.CallStack.toString(haxe.CallStack.exceptionStack()) );
			neko.Lib.rethrow(e);
		}
	}

	function publishLoop(){
		try {
			var err = false;

			try {
				pcnx = new redis.Connection(host,port);
				pcnx.setTimeout(1);
				Tora.log("[Redis-Publish] Connected to "+key+".");
			}catch(e : Dynamic ){
			}

			while( true ){
				if( closing )
					break;
				try {
					var a = pbuffer.pop(true);
					if( closing && a.length == 0 ){
					}else if( pcnx == null ){
						pbuffer.push(a);
						throw "Eof";
					}else if( !connected ){
						pbuffer.push(a);
						Sys.sleep(1);
					}else{
						try {
							var b = Lambda.array(a);
							pcnx.command(b.shift(),b);
						}catch( e : Dynamic ){
							pbuffer.push(a);
							neko.Lib.rethrow(e);
						}
					}
				}catch( e : Dynamic ){
					switch( Std.string(e) ){
						case "Blocked":
						case "Eof","{}": // TODO why Eof become "{}" ??
							try {
								if( !err ){
									err = true;
									Tora.log("[Redis-Publish] "+key+" connection lost. Try to reconnect...");
								}
								pcnx = new redis.Connection(host,port);
								pcnx.setTimeout(1);
								Tora.log("[Redis-Publish] "+key+" re-connected OK.");
								err = false;
							}catch( e : Dynamic ){
								pcnx = null;
								Sys.sleep(1);
							}
						default:
							neko.Lib.rethrow(e);
					}
				}
			}

			if( pcnx != null ){
				try{
					pcnx.close();
				}catch( e : Dynamic ){
				}
				pcnx = null;
				pbuffer = null;
				Tora.log("[Redis-Publish] "+key+" connection closed.");
			}
		}catch(e : Dynamic ){
			Sys.print("Unrecoverable error in redis publishLoop: ");
			Sys.println( e );
			Sys.println( haxe.CallStack.toString(haxe.CallStack.exceptionStack()) );
			neko.Lib.rethrow(e);
		}
	}
	
	function onMessage( chan : String, msg : String ) {
		lock.acquire();
		if( closing )
			return;
		var q = queues.get(chan);
		lock.release();

		if( q == null )
			return;
		
		var unserialize = false;
		var msgData : Dynamic = null;

		q.lock.acquire();
		var ctx = new ModToraApi.ModuleContext();
		for( qc in q.clients ) {
			if( qc.c.needClose ) continue;
			if( !ctx.initHandler(qc) )
				qc.c.needClose = true;

			if( !unserialize ){
				msgData = try qc.h.unserialize( neko.NativeString.ofString(msg) ) catch( e : Dynamic ) null;
				unserialize = true;
			}
			try {
				qc.h.onNotify(msgData);
			} catch( e : Dynamic ) {
				var data = try {
					var stack = haxe.CallStack.callStack().concat(haxe.CallStack.exceptionStack());
					Std.string(e) + haxe.CallStack.toString(stack);
				} catch( _ : Dynamic ) "???";
				try {
					qc.c.sendMessage(tora.Code.CError,data);
				} catch( _ : Dynamic ) {
					qc.c.needClose = true;
				}
			}
			ctx.resetHandler(qc);
			if( qc.c.needClose )
				Tora.inst.close(qc.c, true);
		}
		ctx.restore();
		q.lock.release();
	}

	public function removeQueue( q ){
		if( !queues.exists(q.name) )
			return;
		buffer.add(["UNSUBSCRIBE",q.name]);
		queues.remove( q.name );
	}
	
	public function addQueue( q ){
		if( queues.exists(q.name) )
			return;
		queues.set( q.name, q );
		buffer.add( ["SUBSCRIBE",q.name] );
	}

	public function close(){
		closing = true;
		pbuffer.add([]);
	}
}
