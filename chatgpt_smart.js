#!/usr/bin/env node
var __getOwnPropNames = Object.getOwnPropertyNames;
var __commonJS = (cb, mod) => function __require() {
  try {
    return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
  } catch (e) {
    throw mod = 0, e;
  }
};

// node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "node_modules/ws/lib/constants.js"(exports2, module2) {
    "use strict";
    var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"];
    var hasBlob = typeof Blob !== "undefined";
    if (hasBlob) BINARY_TYPES.push("blob");
    module2.exports = {
      BINARY_TYPES,
      CLOSE_TIMEOUT: 3e4,
      EMPTY_BUFFER: Buffer.alloc(0),
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      hasBlob,
      kForOnEventAttribute: /* @__PURE__ */ Symbol("kIsForOnEventAttribute"),
      kListener: /* @__PURE__ */ Symbol("kListener"),
      kStatusCode: /* @__PURE__ */ Symbol("status-code"),
      kWebSocket: /* @__PURE__ */ Symbol("websocket"),
      NOOP: () => {
      }
    };
  }
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "node_modules/ws/lib/buffer-util.js"(exports2, module2) {
    "use strict";
    var { EMPTY_BUFFER } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    function concat(list, totalLength) {
      if (list.length === 0) return EMPTY_BUFFER;
      if (list.length === 1) return list[0];
      const target = Buffer.allocUnsafe(totalLength);
      let offset = 0;
      for (let i = 0; i < list.length; i++) {
        const buf = list[i];
        target.set(buf, offset);
        offset += buf.length;
      }
      if (offset < totalLength) {
        return new FastBuffer(target.buffer, target.byteOffset, offset);
      }
      return target;
    }
    function _mask(source, mask, output, offset, length) {
      for (let i = 0; i < length; i++) {
        output[offset + i] = source[i] ^ mask[i & 3];
      }
    }
    function _unmask(buffer, mask) {
      for (let i = 0; i < buffer.length; i++) {
        buffer[i] ^= mask[i & 3];
      }
    }
    function toArrayBuffer(buf) {
      if (buf.length === buf.buffer.byteLength) {
        return buf.buffer;
      }
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
    }
    function toBuffer(data) {
      toBuffer.readOnly = true;
      if (Buffer.isBuffer(data)) return data;
      let buf;
      if (data instanceof ArrayBuffer) {
        buf = new FastBuffer(data);
      } else if (ArrayBuffer.isView(data)) {
        buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
      } else {
        buf = Buffer.from(data);
        toBuffer.readOnly = false;
      }
      return buf;
    }
    module2.exports = {
      concat,
      mask: _mask,
      toArrayBuffer,
      toBuffer,
      unmask: _unmask
    };
    if (!process.env.WS_NO_BUFFER_UTIL) {
      try {
        const bufferUtil = require("bufferutil");
        module2.exports.mask = function(source, mask, output, offset, length) {
          if (length < 48) _mask(source, mask, output, offset, length);
          else bufferUtil.mask(source, mask, output, offset, length);
        };
        module2.exports.unmask = function(buffer, mask) {
          if (buffer.length < 32) _unmask(buffer, mask);
          else bufferUtil.unmask(buffer, mask);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/limiter.js
var require_limiter = __commonJS({
  "node_modules/ws/lib/limiter.js"(exports2, module2) {
    "use strict";
    var kDone = /* @__PURE__ */ Symbol("kDone");
    var kRun = /* @__PURE__ */ Symbol("kRun");
    var Limiter = class {
      /**
       * Creates a new `Limiter`.
       *
       * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
       *     to run concurrently
       */
      constructor(concurrency) {
        this[kDone] = () => {
          this.pending--;
          this[kRun]();
        };
        this.concurrency = concurrency || Infinity;
        this.jobs = [];
        this.pending = 0;
      }
      /**
       * Adds a job to the queue.
       *
       * @param {Function} job The job to run
       * @public
       */
      add(job) {
        this.jobs.push(job);
        this[kRun]();
      }
      /**
       * Removes a job from the queue and runs it if possible.
       *
       * @private
       */
      [kRun]() {
        if (this.pending === this.concurrency) return;
        if (this.jobs.length) {
          const job = this.jobs.shift();
          this.pending++;
          job(this[kDone]);
        }
      }
    };
    module2.exports = Limiter;
  }
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "node_modules/ws/lib/permessage-deflate.js"(exports2, module2) {
    "use strict";
    var zlib = require("zlib");
    var bufferUtil = require_buffer_util();
    var Limiter = require_limiter();
    var { kStatusCode } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    var TRAILER = Buffer.from([0, 0, 255, 255]);
    var kPerMessageDeflate = /* @__PURE__ */ Symbol("permessage-deflate");
    var kTotalLength = /* @__PURE__ */ Symbol("total-length");
    var kCallback = /* @__PURE__ */ Symbol("callback");
    var kBuffers = /* @__PURE__ */ Symbol("buffers");
    var kError = /* @__PURE__ */ Symbol("error");
    var zlibLimiter;
    var PerMessageDeflate = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} [options] Configuration options
       * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
       *     for, or request, a custom client window size
       * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
       *     acknowledge disabling of client context takeover
       * @param {Number} [options.concurrencyLimit=10] The number of concurrent
       *     calls to zlib
       * @param {Boolean} [options.isServer=false] Create the instance in either
       *     server or client mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
       *     use of a custom server window size
       * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
       *     disabling of server context takeover
       * @param {Number} [options.threshold=1024] Size (in bytes) below which
       *     messages should not be compressed if context takeover is disabled
       * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
       *     deflate
       * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
       *     inflate
       */
      constructor(options) {
        this._options = options || {};
        this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
        this._maxPayload = this._options.maxPayload | 0;
        this._isServer = !!this._options.isServer;
        this._deflate = null;
        this._inflate = null;
        this.params = null;
        if (!zlibLimiter) {
          const concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter(concurrency);
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        const params = {};
        if (this._options.serverNoContextTakeover) {
          params.server_no_context_takeover = true;
        }
        if (this._options.clientNoContextTakeover) {
          params.client_no_context_takeover = true;
        }
        if (this._options.serverMaxWindowBits) {
          params.server_max_window_bits = this._options.serverMaxWindowBits;
        }
        if (this._options.clientMaxWindowBits) {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        } else if (this._options.clientMaxWindowBits == null) {
          params.client_max_window_bits = true;
        }
        return params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        configurations = this.normalizeParams(configurations);
        this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
        return this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate) {
          this._inflate.close();
          this._inflate = null;
        }
        if (this._deflate) {
          const callback = this._deflate[kCallback];
          this._deflate.close();
          this._deflate = null;
          if (callback) {
            callback(
              new Error(
                "The deflate stream was closed while data was being processed"
              )
            );
          }
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        const opts = this._options;
        const accepted = offers.find((params) => {
          if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && (typeof params.client_max_window_bits === "number" ? opts.clientMaxWindowBits > params.client_max_window_bits : !params.client_max_window_bits)) {
            return false;
          }
          return true;
        });
        if (!accepted) {
          throw new Error("None of the extension offers can be accepted");
        }
        if (opts.serverNoContextTakeover) {
          accepted.server_no_context_takeover = true;
        }
        if (opts.clientNoContextTakeover) {
          accepted.client_no_context_takeover = true;
        }
        if (typeof opts.serverMaxWindowBits === "number") {
          accepted.server_max_window_bits = opts.serverMaxWindowBits;
        }
        if (typeof opts.clientMaxWindowBits === "number") {
          accepted.client_max_window_bits = opts.clientMaxWindowBits;
        } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
          delete accepted.client_max_window_bits;
        }
        return accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        const params = response[0];
        if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        }
        if (!params.client_max_window_bits) {
          if (typeof this._options.clientMaxWindowBits === "number") {
            params.client_max_window_bits = this._options.clientMaxWindowBits;
          }
        } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        }
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            let value = params[key];
            if (value.length > 1) {
              throw new Error(`Parameter "${key}" must have only a single value`);
            }
            value = value[0];
            if (key === "client_max_window_bits") {
              if (value !== true) {
                const num = +value;
                if (!Number.isInteger(num) || num < 8 || num > 15) {
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                }
                value = num;
              } else if (!this._isServer) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else if (key === "server_max_window_bits") {
              const num = +value;
              if (!Number.isInteger(num) || num < 8 || num > 15) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
              value = num;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== true) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else {
              throw new Error(`Unknown parameter "${key}"`);
            }
            params[key] = value;
          });
        });
        return configurations;
      }
      /**
       * Decompress data. Concurrency limited.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._decompress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Compress data. Concurrency limited.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._compress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        const endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib.createInflateRaw({
            ...this._options.zlibInflateOptions,
            windowBits
          });
          this._inflate[kPerMessageDeflate] = this;
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          this._inflate.on("error", inflateOnError);
          this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback;
        this._inflate.write(data);
        if (fin) this._inflate.write(TRAILER);
        this._inflate.flush(() => {
          const err = this._inflate[kError];
          if (err) {
            this._inflate.close();
            this._inflate = null;
            callback(err);
            return;
          }
          const data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          if (this._inflate._readableState.endEmitted) {
            this._inflate.close();
            this._inflate = null;
          } else {
            this._inflate[kTotalLength] = 0;
            this._inflate[kBuffers] = [];
            if (fin && this.params[`${endpoint}_no_context_takeover`]) {
              this._inflate.reset();
            }
          }
          callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        const endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib.createDeflateRaw({
            ...this._options.zlibDeflateOptions,
            windowBits
          });
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          this._deflate.on("data", deflateOnData);
        }
        this._deflate[kCallback] = callback;
        this._deflate.write(data);
        this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
          if (!this._deflate) {
            return;
          }
          let data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          if (fin) {
            data2 = new FastBuffer(data2.buffer, data2.byteOffset, data2.length - 4);
          }
          this._deflate[kCallback] = null;
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          if (fin && this.params[`${endpoint}_no_context_takeover`]) {
            this._deflate.reset();
          }
          callback(null, data2);
        });
      }
    };
    module2.exports = PerMessageDeflate;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk);
      this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      this[kTotalLength] += chunk.length;
      if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded");
      this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
      this[kError][kStatusCode] = 1009;
      this.removeListener("data", inflateOnData);
      this.reset();
    }
    function inflateOnError(err) {
      this[kPerMessageDeflate]._inflate = null;
      if (this[kError]) {
        this[kCallback](this[kError]);
        return;
      }
      err[kStatusCode] = 1007;
      this[kCallback](err);
    }
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "node_modules/ws/lib/validation.js"(exports2, module2) {
    "use strict";
    var { isUtf8 } = require("buffer");
    var { hasBlob } = require_constants();
    var tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function isValidStatusCode(code) {
      return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    }
    function _isValidUTF8(buf) {
      const len = buf.length;
      let i = 0;
      while (i < len) {
        if ((buf[i] & 128) === 0) {
          i++;
        } else if ((buf[i] & 224) === 192) {
          if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) {
            return false;
          }
          i += 2;
        } else if ((buf[i] & 240) === 224) {
          if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || // Overlong
          buf[i] === 237 && (buf[i + 1] & 224) === 160) {
            return false;
          }
          i += 3;
        } else if ((buf[i] & 248) === 240) {
          if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || // Overlong
          buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) {
            return false;
          }
          i += 4;
        } else {
          return false;
        }
      }
      return true;
    }
    function isBlob(value) {
      return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
    }
    module2.exports = {
      isBlob,
      isValidStatusCode,
      isValidUTF8: _isValidUTF8,
      tokenChars
    };
    if (isUtf8) {
      module2.exports.isValidUTF8 = function(buf) {
        return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
      };
    } else if (!process.env.WS_NO_UTF_8_VALIDATE) {
      try {
        const isValidUTF8 = require("utf-8-validate");
        module2.exports.isValidUTF8 = function(buf) {
          return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "node_modules/ws/lib/receiver.js"(exports2, module2) {
    "use strict";
    var { Writable } = require("stream");
    var PerMessageDeflate = require_permessage_deflate();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants();
    var { concat, toArrayBuffer, unmask } = require_buffer_util();
    var { isValidStatusCode, isValidUTF8 } = require_validation();
    var FastBuffer = Buffer[Symbol.species];
    var GET_INFO = 0;
    var GET_PAYLOAD_LENGTH_16 = 1;
    var GET_PAYLOAD_LENGTH_64 = 2;
    var GET_MASK = 3;
    var GET_DATA = 4;
    var INFLATING = 5;
    var DEFER_EVENT = 6;
    var Receiver = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {Object} [options] Options object
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {String} [options.binaryType=nodebuffer] The type for binary data
       * @param {Object} [options.extensions] An object containing the negotiated
       *     extensions
       * @param {Boolean} [options.isServer=false] Specifies whether to operate in
       *     client or server mode
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       */
      constructor(options = {}) {
        super();
        this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : true;
        this._binaryType = options.binaryType || BINARY_TYPES[0];
        this._extensions = options.extensions || {};
        this._isServer = !!options.isServer;
        this._maxBufferedChunks = options.maxBufferedChunks | 0;
        this._maxFragments = options.maxFragments | 0;
        this._maxPayload = options.maxPayload | 0;
        this._skipUTF8Validation = !!options.skipUTF8Validation;
        this[kWebSocket] = void 0;
        this._bufferedBytes = 0;
        this._buffers = [];
        this._compressed = false;
        this._payloadLength = 0;
        this._mask = void 0;
        this._fragmented = 0;
        this._masked = false;
        this._fin = false;
        this._opcode = 0;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._numFragments = 0;
        this._fragments = [];
        this._errored = false;
        this._loop = false;
        this._state = GET_INFO;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       * @private
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO) return cb();
        if (this._maxBufferedChunks > 0 && this._buffers.length >= this._maxBufferedChunks) {
          cb(
            this.createError(
              RangeError,
              "Too many buffered chunks",
              false,
              1008,
              "WS_ERR_TOO_MANY_BUFFERED_PARTS"
            )
          );
          return;
        }
        this._bufferedBytes += chunk.length;
        this._buffers.push(chunk);
        this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        this._bufferedBytes -= n;
        if (n === this._buffers[0].length) return this._buffers.shift();
        if (n < this._buffers[0].length) {
          const buf = this._buffers[0];
          this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          );
          return new FastBuffer(buf.buffer, buf.byteOffset, n);
        }
        const dst = Buffer.allocUnsafe(n);
        do {
          const buf = this._buffers[0];
          const offset = dst.length - n;
          if (n >= buf.length) {
            dst.set(this._buffers.shift(), offset);
          } else {
            dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
            this._buffers[0] = new FastBuffer(
              buf.buffer,
              buf.byteOffset + n,
              buf.length - n
            );
          }
          n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        this._loop = true;
        do {
          switch (this._state) {
            case GET_INFO:
              this.getInfo(cb);
              break;
            case GET_PAYLOAD_LENGTH_16:
              this.getPayloadLength16(cb);
              break;
            case GET_PAYLOAD_LENGTH_64:
              this.getPayloadLength64(cb);
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              this.getData(cb);
              break;
            case INFLATING:
            case DEFER_EVENT:
              this._loop = false;
              return;
          }
        } while (this._loop);
        if (!this._errored) cb();
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @param {Function} cb Callback
       * @private
       */
      getInfo(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        const buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          const error = this.createError(
            RangeError,
            "RSV2 and RSV3 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_2_3"
          );
          cb(error);
          return;
        }
        const compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate.extensionName]) {
          const error = this.createError(
            RangeError,
            "RSV1 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_1"
          );
          cb(error);
          return;
        }
        this._fin = (buf[0] & 128) === 128;
        this._opcode = buf[0] & 15;
        this._payloadLength = buf[1] & 127;
        if (this._opcode === 0) {
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (!this._fragmented) {
            const error = this.createError(
              RangeError,
              "invalid opcode 0",
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            const error = this.createError(
              RangeError,
              `invalid opcode ${this._opcode}`,
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            const error = this.createError(
              RangeError,
              "FIN must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_FIN"
            );
            cb(error);
            return;
          }
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
            const error = this.createError(
              RangeError,
              `invalid payload length ${this._payloadLength}`,
              true,
              1002,
              "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"
            );
            cb(error);
            return;
          }
        } else {
          const error = this.createError(
            RangeError,
            `invalid opcode ${this._opcode}`,
            true,
            1002,
            "WS_ERR_INVALID_OPCODE"
          );
          cb(error);
          return;
        }
        if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
        this._masked = (buf[1] & 128) === 128;
        if (this._isServer) {
          if (!this._masked) {
            const error = this.createError(
              RangeError,
              "MASK must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_MASK"
            );
            cb(error);
            return;
          }
        } else if (this._masked) {
          const error = this.createError(
            RangeError,
            "MASK must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_MASK"
          );
          cb(error);
          return;
        }
        if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
        else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
        else this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength16(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0);
        this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength64(cb) {
        if (this._bufferedBytes < 8) {
          this._loop = false;
          return;
        }
        const buf = this.consume(8);
        const num = buf.readUInt32BE(0);
        if (num > Math.pow(2, 53 - 32) - 1) {
          const error = this.createError(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            false,
            1009,
            "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"
          );
          cb(error);
          return;
        }
        this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
        this.haveLength(cb);
      }
      /**
       * Payload length has been read.
       *
       * @param {Function} cb Callback
       * @private
       */
      haveLength(cb) {
        if (this._payloadLength && this._opcode < 8) {
          this._totalPayloadLength += this._payloadLength;
          if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
            const error = this.createError(
              RangeError,
              "Max payload size exceeded",
              false,
              1009,
              "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
            );
            cb(error);
            return;
          }
        }
        if (this._masked) this._state = GET_MASK;
        else this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = false;
          return;
        }
        this._mask = this.consume(4);
        this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @private
       */
      getData(cb) {
        let data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = false;
            return;
          }
          data = this.consume(this._payloadLength);
          if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) {
            unmask(data, this._mask);
          }
        }
        if (this._opcode > 7) {
          this.controlMessage(data, cb);
          return;
        }
        if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
          const error = this.createError(
            RangeError,
            "Too many message fragments",
            false,
            1008,
            "WS_ERR_TOO_MANY_BUFFERED_PARTS"
          );
          cb(error);
          return;
        }
        if (this._compressed) {
          this._state = INFLATING;
          this.decompress(data, cb);
          return;
        }
        if (data.length) {
          this._messageLength = this._totalPayloadLength;
          this._fragments.push(data);
        }
        this.dataMessage(cb);
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
        perMessageDeflate.decompress(data, this._fin, (err, buf) => {
          if (err) return cb(err);
          if (buf.length) {
            this._messageLength += buf.length;
            if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
              const error = this.createError(
                RangeError,
                "Max payload size exceeded",
                false,
                1009,
                "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
              );
              cb(error);
              return;
            }
            this._fragments.push(buf);
          }
          this.dataMessage(cb);
          if (this._state === GET_INFO) this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @param {Function} cb Callback
       * @private
       */
      dataMessage(cb) {
        if (!this._fin) {
          this._state = GET_INFO;
          return;
        }
        const messageLength = this._messageLength;
        const fragments = this._fragments;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragmented = 0;
        this._numFragments = 0;
        this._fragments = [];
        if (this._opcode === 2) {
          let data;
          if (this._binaryType === "nodebuffer") {
            data = concat(fragments, messageLength);
          } else if (this._binaryType === "arraybuffer") {
            data = toArrayBuffer(concat(fragments, messageLength));
          } else if (this._binaryType === "blob") {
            data = new Blob(fragments);
          } else {
            data = fragments;
          }
          if (this._allowSynchronousEvents) {
            this.emit("message", data, true);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", data, true);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        } else {
          const buf = concat(fragments, messageLength);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            const error = this.createError(
              Error,
              "invalid UTF-8 sequence",
              true,
              1007,
              "WS_ERR_INVALID_UTF8"
            );
            cb(error);
            return;
          }
          if (this._state === INFLATING || this._allowSynchronousEvents) {
            this.emit("message", buf, false);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", buf, false);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        }
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data, cb) {
        if (this._opcode === 8) {
          if (data.length === 0) {
            this._loop = false;
            this.emit("conclude", 1005, EMPTY_BUFFER);
            this.end();
          } else {
            const code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              const error = this.createError(
                RangeError,
                `invalid status code ${code}`,
                true,
                1002,
                "WS_ERR_INVALID_CLOSE_CODE"
              );
              cb(error);
              return;
            }
            const buf = new FastBuffer(
              data.buffer,
              data.byteOffset + 2,
              data.length - 2
            );
            if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
              const error = this.createError(
                Error,
                "invalid UTF-8 sequence",
                true,
                1007,
                "WS_ERR_INVALID_UTF8"
              );
              cb(error);
              return;
            }
            this._loop = false;
            this.emit("conclude", code, buf);
            this.end();
          }
          this._state = GET_INFO;
          return;
        }
        if (this._allowSynchronousEvents) {
          this.emit(this._opcode === 9 ? "ping" : "pong", data);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit(this._opcode === 9 ? "ping" : "pong", data);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      }
      /**
       * Builds an error object.
       *
       * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
       * @param {String} message The error message
       * @param {Boolean} prefix Specifies whether or not to add a default prefix to
       *     `message`
       * @param {Number} statusCode The status code
       * @param {String} errorCode The exposed error code
       * @return {(Error|RangeError)} The error
       * @private
       */
      createError(ErrorCtor, message, prefix, statusCode, errorCode) {
        this._loop = false;
        this._errored = true;
        const err = new ErrorCtor(
          prefix ? `Invalid WebSocket frame: ${message}` : message
        );
        Error.captureStackTrace(err, this.createError);
        err.code = errorCode;
        err[kStatusCode] = statusCode;
        return err;
      }
    };
    module2.exports = Receiver;
  }
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "node_modules/ws/lib/sender.js"(exports2, module2) {
    "use strict";
    var { Duplex } = require("stream");
    var { randomFillSync } = require("crypto");
    var {
      types: { isUint8Array }
    } = require("util");
    var PerMessageDeflate = require_permessage_deflate();
    var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants();
    var { isBlob, isValidStatusCode } = require_validation();
    var { mask: applyMask, toBuffer } = require_buffer_util();
    var kByteLength = /* @__PURE__ */ Symbol("kByteLength");
    var maskBuffer = Buffer.alloc(4);
    var RANDOM_POOL_SIZE = 8 * 1024;
    var randomPool;
    var randomPoolPointer = RANDOM_POOL_SIZE;
    var DEFAULT = 0;
    var DEFLATING = 1;
    var GET_BLOB_DATA = 2;
    var Sender = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {Duplex} socket The connection socket
       * @param {Object} [extensions] An object containing the negotiated extensions
       * @param {Function} [generateMask] The function used to generate the masking
       *     key
       */
      constructor(socket, extensions, generateMask) {
        this._extensions = extensions || {};
        if (generateMask) {
          this._generateMask = generateMask;
          this._maskBuffer = Buffer.alloc(4);
        }
        this._socket = socket;
        this._firstFragment = true;
        this._compress = false;
        this._bufferedBytes = 0;
        this._queue = [];
        this._state = DEFAULT;
        this.onerror = NOOP;
        this[kWebSocket] = void 0;
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {(Buffer|String)} data The data to frame
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @return {(Buffer|String)[]} The framed data
       * @public
       */
      static frame(data, options) {
        let mask;
        let merge = false;
        let offset = 2;
        let skipMasking = false;
        if (options.mask) {
          mask = options.maskBuffer || maskBuffer;
          if (options.generateMask) {
            options.generateMask(mask);
          } else {
            if (randomPoolPointer === RANDOM_POOL_SIZE) {
              if (randomPool === void 0) {
                randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
              }
              randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
              randomPoolPointer = 0;
            }
            mask[0] = randomPool[randomPoolPointer++];
            mask[1] = randomPool[randomPoolPointer++];
            mask[2] = randomPool[randomPoolPointer++];
            mask[3] = randomPool[randomPoolPointer++];
          }
          skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
          offset = 6;
        }
        let dataLength;
        if (typeof data === "string") {
          if ((!options.mask || skipMasking) && options[kByteLength] !== void 0) {
            dataLength = options[kByteLength];
          } else {
            data = Buffer.from(data);
            dataLength = data.length;
          }
        } else {
          dataLength = data.length;
          merge = options.mask && options.readOnly && !skipMasking;
        }
        let payloadLength = dataLength;
        if (dataLength >= 65536) {
          offset += 8;
          payloadLength = 127;
        } else if (dataLength > 125) {
          offset += 2;
          payloadLength = 126;
        }
        const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
        target[0] = options.fin ? options.opcode | 128 : options.opcode;
        if (options.rsv1) target[0] |= 64;
        target[1] = payloadLength;
        if (payloadLength === 126) {
          target.writeUInt16BE(dataLength, 2);
        } else if (payloadLength === 127) {
          target[2] = target[3] = 0;
          target.writeUIntBE(dataLength, 4, 6);
        }
        if (!options.mask) return [target, data];
        target[1] |= 128;
        target[offset - 4] = mask[0];
        target[offset - 3] = mask[1];
        target[offset - 2] = mask[2];
        target[offset - 1] = mask[3];
        if (skipMasking) return [target, data];
        if (merge) {
          applyMask(data, mask, target, offset, dataLength);
          return [target];
        }
        applyMask(data, mask, data, 0, dataLength);
        return [target, data];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {Number} [code] The status code component of the body
       * @param {(String|Buffer)} [data] The message component of the body
       * @param {Boolean} [mask=false] Specifies whether or not to mask the message
       * @param {Function} [cb] Callback
       * @public
       */
      close(code, data, mask, cb) {
        let buf;
        if (code === void 0) {
          buf = EMPTY_BUFFER;
        } else if (typeof code !== "number" || !isValidStatusCode(code)) {
          throw new TypeError("First argument must be a valid error code number");
        } else if (data === void 0 || !data.length) {
          buf = Buffer.allocUnsafe(2);
          buf.writeUInt16BE(code, 0);
        } else {
          const length = Buffer.byteLength(data);
          if (length > 123) {
            throw new RangeError("The message must not be greater than 123 bytes");
          }
          buf = Buffer.allocUnsafe(2 + length);
          buf.writeUInt16BE(code, 0);
          if (typeof data === "string") {
            buf.write(data, 2);
          } else if (isUint8Array(data)) {
            buf.set(data, 2);
          } else {
            throw new TypeError("Second argument must be a string or a Uint8Array");
          }
        }
        const options = {
          [kByteLength]: buf.length,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 8,
          readOnly: false,
          rsv1: false
        };
        if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, buf, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(buf, options), cb);
        }
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      ping(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 9,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      pong(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 10,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
       *     or text
       * @param {Boolean} [options.compress=false] Specifies whether or not to
       *     compress `data`
       * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Function} [cb] Callback
       * @public
       */
      send(data, options, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
        let opcode = options.binary ? 2 : 1;
        let rsv1 = options.compress;
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (this._firstFragment) {
          this._firstFragment = false;
          if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) {
            rsv1 = byteLength >= perMessageDeflate._threshold;
          }
          this._compress = rsv1;
        } else {
          rsv1 = false;
          opcode = 0;
        }
        if (options.fin) this._firstFragment = true;
        const opts = {
          [kByteLength]: byteLength,
          fin: options.fin,
          generateMask: this._generateMask,
          mask: options.mask,
          maskBuffer: this._maskBuffer,
          opcode,
          readOnly,
          rsv1
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
          } else {
            this.getBlobData(data, this._compress, opts, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, this._compress, opts, cb]);
        } else {
          this.dispatch(data, this._compress, opts, cb);
        }
      }
      /**
       * Gets the contents of a blob as binary data.
       *
       * @param {Blob} blob The blob
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     the data
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      getBlobData(blob, compress, options, cb) {
        this._bufferedBytes += options[kByteLength];
        this._state = GET_BLOB_DATA;
        blob.arrayBuffer().then((arrayBuffer) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while the blob was being read"
            );
            process.nextTick(callCallbacks, this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          const data = toBuffer(arrayBuffer);
          if (!compress) {
            this._state = DEFAULT;
            this.sendFrame(_Sender.frame(data, options), cb);
            this.dequeue();
          } else {
            this.dispatch(data, compress, options, cb);
          }
        }).catch((err) => {
          process.nextTick(onError, this, err, cb);
        });
      }
      /**
       * Dispatches a message.
       *
       * @param {(Buffer|String)} data The message to send
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     `data`
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
        this._bufferedBytes += options[kByteLength];
        this._state = DEFLATING;
        perMessageDeflate.compress(data, options.fin, (_, buf) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while data was being compressed"
            );
            callCallbacks(this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          this._state = DEFAULT;
          options.readOnly = false;
          this.sendFrame(_Sender.frame(buf, options), cb);
          this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        while (this._state === DEFAULT && this._queue.length) {
          const params = this._queue.shift();
          this._bufferedBytes -= params[3][kByteLength];
          Reflect.apply(params[0], this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[3][kByteLength];
        this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {(Buffer | String)[]} list The frame to send
       * @param {Function} [cb] Callback
       * @private
       */
      sendFrame(list, cb) {
        if (list.length === 2) {
          this._socket.cork();
          this._socket.write(list[0]);
          this._socket.write(list[1], cb);
          this._socket.uncork();
        } else {
          this._socket.write(list[0], cb);
        }
      }
    };
    module2.exports = Sender;
    function callCallbacks(sender, err, cb) {
      if (typeof cb === "function") cb(err);
      for (let i = 0; i < sender._queue.length; i++) {
        const params = sender._queue[i];
        const callback = params[params.length - 1];
        if (typeof callback === "function") callback(err);
      }
    }
    function onError(sender, err, cb) {
      callCallbacks(sender, err, cb);
      sender.onerror(err);
    }
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "node_modules/ws/lib/event-target.js"(exports2, module2) {
    "use strict";
    var { kForOnEventAttribute, kListener } = require_constants();
    var kCode = /* @__PURE__ */ Symbol("kCode");
    var kData = /* @__PURE__ */ Symbol("kData");
    var kError = /* @__PURE__ */ Symbol("kError");
    var kMessage = /* @__PURE__ */ Symbol("kMessage");
    var kReason = /* @__PURE__ */ Symbol("kReason");
    var kTarget = /* @__PURE__ */ Symbol("kTarget");
    var kType = /* @__PURE__ */ Symbol("kType");
    var kWasClean = /* @__PURE__ */ Symbol("kWasClean");
    var Event = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @throws {TypeError} If the `type` argument is not specified
       */
      constructor(type) {
        this[kTarget] = null;
        this[kType] = type;
      }
      /**
       * @type {*}
       */
      get target() {
        return this[kTarget];
      }
      /**
       * @type {String}
       */
      get type() {
        return this[kType];
      }
    };
    Object.defineProperty(Event.prototype, "target", { enumerable: true });
    Object.defineProperty(Event.prototype, "type", { enumerable: true });
    var CloseEvent = class extends Event {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {Number} [options.code=0] The status code explaining why the
       *     connection was closed
       * @param {String} [options.reason=''] A human-readable string explaining why
       *     the connection was closed
       * @param {Boolean} [options.wasClean=false] Indicates whether or not the
       *     connection was cleanly closed
       */
      constructor(type, options = {}) {
        super(type);
        this[kCode] = options.code === void 0 ? 0 : options.code;
        this[kReason] = options.reason === void 0 ? "" : options.reason;
        this[kWasClean] = options.wasClean === void 0 ? false : options.wasClean;
      }
      /**
       * @type {Number}
       */
      get code() {
        return this[kCode];
      }
      /**
       * @type {String}
       */
      get reason() {
        return this[kReason];
      }
      /**
       * @type {Boolean}
       */
      get wasClean() {
        return this[kWasClean];
      }
    };
    Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });
    var ErrorEvent = class extends Event {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.error=null] The error that generated this event
       * @param {String} [options.message=''] The error message
       */
      constructor(type, options = {}) {
        super(type);
        this[kError] = options.error === void 0 ? null : options.error;
        this[kMessage] = options.message === void 0 ? "" : options.message;
      }
      /**
       * @type {*}
       */
      get error() {
        return this[kError];
      }
      /**
       * @type {String}
       */
      get message() {
        return this[kMessage];
      }
    };
    Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
    Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });
    var MessageEvent = class extends Event {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.data=null] The message content
       */
      constructor(type, options = {}) {
        super(type);
        this[kData] = options.data === void 0 ? null : options.data;
      }
      /**
       * @type {*}
       */
      get data() {
        return this[kData];
      }
    };
    Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} type A string representing the event type to listen for
       * @param {(Function|Object)} handler The listener to add
       * @param {Object} [options] An options object specifies characteristics about
       *     the event listener
       * @param {Boolean} [options.once=false] A `Boolean` indicating that the
       *     listener should be invoked at most once after being added. If `true`,
       *     the listener would be automatically removed when invoked.
       * @public
       */
      addEventListener(type, handler, options = {}) {
        for (const listener of this.listeners(type)) {
          if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            return;
          }
        }
        let wrapper;
        if (type === "message") {
          wrapper = function onMessage(data, isBinary) {
            const event = new MessageEvent("message", {
              data: isBinary ? data : data.toString()
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "close") {
          wrapper = function onClose(code, message) {
            const event = new CloseEvent("close", {
              code,
              reason: message.toString(),
              wasClean: this._closeFrameReceived && this._closeFrameSent
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "error") {
          wrapper = function onError(error) {
            const event = new ErrorEvent("error", {
              error,
              message: error.message
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "open") {
          wrapper = function onOpen() {
            const event = new Event("open");
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else {
          return;
        }
        wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
        wrapper[kListener] = handler;
        if (options.once) {
          this.once(type, wrapper);
        } else {
          this.on(type, wrapper);
        }
      },
      /**
       * Remove an event listener.
       *
       * @param {String} type A string representing the event type to remove
       * @param {(Function|Object)} handler The listener to remove
       * @public
       */
      removeEventListener(type, handler) {
        for (const listener of this.listeners(type)) {
          if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            this.removeListener(type, listener);
            break;
          }
        }
      }
    };
    module2.exports = {
      CloseEvent,
      ErrorEvent,
      Event,
      EventTarget,
      MessageEvent
    };
    function callListener(listener, thisArg, event) {
      if (typeof listener === "object" && listener.handleEvent) {
        listener.handleEvent.call(listener, event);
      } else {
        listener.call(thisArg, event);
      }
    }
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "node_modules/ws/lib/extension.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function push(dest, name, elem) {
      if (dest[name] === void 0) dest[name] = [elem];
      else dest[name].push(elem);
    }
    function parse(header) {
      const offers = /* @__PURE__ */ Object.create(null);
      let params = /* @__PURE__ */ Object.create(null);
      let mustUnescape = false;
      let isEscaping = false;
      let inQuotes = false;
      let extensionName;
      let paramName;
      let start = -1;
      let code = -1;
      let end = -1;
      let i = 0;
      for (; i < header.length; i++) {
        code = header.charCodeAt(i);
        if (extensionName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (i !== 0 && (code === 32 || code === 9)) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            const name = header.slice(start, end);
            if (code === 44) {
              push(offers, name, params);
              params = /* @__PURE__ */ Object.create(null);
            } else {
              extensionName = name;
            }
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else if (paramName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (code === 32 || code === 9) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            push(params, header.slice(start, end), true);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            start = end = -1;
          } else if (code === 61 && start !== -1 && end === -1) {
            paramName = header.slice(start, i);
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else {
          if (isEscaping) {
            if (tokenChars[code] !== 1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (start === -1) start = i;
            else if (!mustUnescape) mustUnescape = true;
            isEscaping = false;
          } else if (inQuotes) {
            if (tokenChars[code] === 1) {
              if (start === -1) start = i;
            } else if (code === 34 && start !== -1) {
              inQuotes = false;
              end = i;
            } else if (code === 92) {
              isEscaping = true;
            } else {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
          } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
            inQuotes = true;
          } else if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (start !== -1 && (code === 32 || code === 9)) {
            if (end === -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            let value = header.slice(start, end);
            if (mustUnescape) {
              value = value.replace(/\\/g, "");
              mustUnescape = false;
            }
            push(params, paramName, value);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            paramName = void 0;
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        }
      }
      if (start === -1 || inQuotes || code === 32 || code === 9) {
        throw new SyntaxError("Unexpected end of input");
      }
      if (end === -1) end = i;
      const token = header.slice(start, end);
      if (extensionName === void 0) {
        push(offers, token, params);
      } else {
        if (paramName === void 0) {
          push(params, token, true);
        } else if (mustUnescape) {
          push(params, paramName, token.replace(/\\/g, ""));
        } else {
          push(params, paramName, token);
        }
        push(offers, extensionName, params);
      }
      return offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension) => {
        let configurations = extensions[extension];
        if (!Array.isArray(configurations)) configurations = [configurations];
        return configurations.map((params) => {
          return [extension].concat(
            Object.keys(params).map((k) => {
              let values = params[k];
              if (!Array.isArray(values)) values = [values];
              return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
            })
          ).join("; ");
        }).join(", ");
      }).join(", ");
    }
    module2.exports = { format, parse };
  }
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "node_modules/ws/lib/websocket.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var https = require("https");
    var http2 = require("http");
    var net = require("net");
    var tls = require("tls");
    var { randomBytes, createHash } = require("crypto");
    var { Duplex, Readable } = require("stream");
    var { URL: URL2 } = require("url");
    var PerMessageDeflate = require_permessage_deflate();
    var Receiver = require_receiver();
    var Sender = require_sender();
    var { isBlob } = require_validation();
    var {
      BINARY_TYPES,
      CLOSE_TIMEOUT,
      EMPTY_BUFFER,
      GUID,
      kForOnEventAttribute,
      kListener,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants();
    var {
      EventTarget: { addEventListener, removeEventListener }
    } = require_event_target();
    var { format, parse } = require_extension();
    var { toBuffer } = require_buffer_util();
    var kAborted = /* @__PURE__ */ Symbol("kAborted");
    var protocolVersions = [8, 13];
    var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
    var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;
    var WebSocket2 = class _WebSocket extends EventEmitter {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|URL)} address The URL to which to connect
       * @param {(String|String[])} [protocols] The subprotocols
       * @param {Object} [options] Connection options
       */
      constructor(address, protocols, options) {
        super();
        this._binaryType = BINARY_TYPES[0];
        this._closeCode = 1006;
        this._closeFrameReceived = false;
        this._closeFrameSent = false;
        this._closeMessage = EMPTY_BUFFER;
        this._closeTimer = null;
        this._errorEmitted = false;
        this._extensions = {};
        this._paused = false;
        this._protocol = "";
        this._readyState = _WebSocket.CONNECTING;
        this._receiver = null;
        this._sender = null;
        this._socket = null;
        if (address !== null) {
          this._bufferedAmount = 0;
          this._isServer = false;
          this._redirects = 0;
          if (protocols === void 0) {
            protocols = [];
          } else if (!Array.isArray(protocols)) {
            if (typeof protocols === "object" && protocols !== null) {
              options = protocols;
              protocols = [];
            } else {
              protocols = [protocols];
            }
          }
          initAsClient(this, address, protocols, options);
        } else {
          this._autoPong = options.autoPong;
          this._closeTimeout = options.closeTimeout;
          this._isServer = true;
        }
      }
      /**
       * For historical reasons, the custom "nodebuffer" type is used by the default
       * instead of "blob".
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        if (!BINARY_TYPES.includes(type)) return;
        this._binaryType = type;
        if (this._receiver) this._receiver._binaryType = type;
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        if (!this._socket) return this._bufferedAmount;
        return this._socket._writableState.length + this._sender._bufferedBytes;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * @type {Boolean}
       */
      get isPaused() {
        return this._paused;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onclose() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onerror() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onopen() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onmessage() {
        return null;
      }
      /**
       * @type {String}
       */
      get protocol() {
        return this._protocol;
      }
      /**
       * @type {Number}
       */
      get readyState() {
        return this._readyState;
      }
      /**
       * @type {String}
       */
      get url() {
        return this._url;
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Object} options Options object
       * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message size
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @private
       */
      setSocket(socket, head, options) {
        const receiver = new Receiver({
          allowSynchronousEvents: options.allowSynchronousEvents,
          binaryType: this.binaryType,
          extensions: this._extensions,
          isServer: this._isServer,
          maxBufferedChunks: options.maxBufferedChunks,
          maxFragments: options.maxFragments,
          maxPayload: options.maxPayload,
          skipUTF8Validation: options.skipUTF8Validation
        });
        const sender = new Sender(socket, this._extensions, options.generateMask);
        this._receiver = receiver;
        this._sender = sender;
        this._socket = socket;
        receiver[kWebSocket] = this;
        sender[kWebSocket] = this;
        socket[kWebSocket] = this;
        receiver.on("conclude", receiverOnConclude);
        receiver.on("drain", receiverOnDrain);
        receiver.on("error", receiverOnError);
        receiver.on("message", receiverOnMessage);
        receiver.on("ping", receiverOnPing);
        receiver.on("pong", receiverOnPong);
        sender.onerror = senderOnError;
        if (socket.setTimeout) socket.setTimeout(0);
        if (socket.setNoDelay) socket.setNoDelay();
        if (head.length > 0) socket.unshift(head);
        socket.on("close", socketOnClose);
        socket.on("data", socketOnData);
        socket.on("end", socketOnEnd);
        socket.on("error", socketOnError);
        this._readyState = _WebSocket.OPEN;
        this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        if (!this._socket) {
          this._readyState = _WebSocket.CLOSED;
          this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        if (this._extensions[PerMessageDeflate.extensionName]) {
          this._extensions[PerMessageDeflate.extensionName].cleanup();
        }
        this._receiver.removeAllListeners();
        this._readyState = _WebSocket.CLOSED;
        this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} [code] Status code explaining why the connection is closing
       * @param {(String|Buffer)} [data] The reason why the connection is
       *     closing
       * @public
       */
      close(code, data) {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this.readyState === _WebSocket.CLOSING) {
          if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) {
            this._socket.end();
          }
          return;
        }
        this._readyState = _WebSocket.CLOSING;
        this._sender.close(code, data, !this._isServer, (err) => {
          if (err) return;
          this._closeFrameSent = true;
          if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) {
            this._socket.end();
          }
        });
        setCloseTimer(this);
      }
      /**
       * Pause the socket.
       *
       * @public
       */
      pause() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = true;
        this._socket.pause();
      }
      /**
       * Send a ping.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Resume the socket.
       *
       * @public
       */
      resume() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = false;
        if (!this._receiver._writableState.needDrain) this._socket.resume();
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} [options] Options object
       * @param {Boolean} [options.binary] Specifies whether `data` is binary or
       *     text
       * @param {Boolean} [options.compress] Specifies whether or not to compress
       *     `data`
       * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof options === "function") {
          cb = options;
          options = {};
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        const opts = {
          binary: typeof data !== "string",
          mask: !this._isServer,
          compress: true,
          fin: true,
          ...options
        };
        if (!this._extensions[PerMessageDeflate.extensionName]) {
          opts.compress = false;
        }
        this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this._socket) {
          this._readyState = _WebSocket.CLOSING;
          this._socket.destroy();
        }
      }
    };
    Object.defineProperty(WebSocket2, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2.prototype, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2.prototype, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    [
      "binaryType",
      "bufferedAmount",
      "extensions",
      "isPaused",
      "protocol",
      "readyState",
      "url"
    ].forEach((property) => {
      Object.defineProperty(WebSocket2.prototype, property, { enumerable: true });
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket2.prototype, `on${method}`, {
        enumerable: true,
        get() {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) return listener[kListener];
          }
          return null;
        },
        set(handler) {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) {
              this.removeListener(method, listener);
              break;
            }
          }
          if (typeof handler !== "function") return;
          this.addEventListener(method, handler, {
            [kForOnEventAttribute]: true
          });
        }
      });
    });
    WebSocket2.prototype.addEventListener = addEventListener;
    WebSocket2.prototype.removeEventListener = removeEventListener;
    module2.exports = WebSocket2;
    function initAsClient(websocket, address, protocols, options) {
      const opts = {
        allowSynchronousEvents: true,
        autoPong: true,
        closeTimeout: CLOSE_TIMEOUT,
        protocolVersion: protocolVersions[1],
        maxBufferedChunks: 256 * 1024,
        maxFragments: 16 * 1024,
        maxPayload: 100 * 1024 * 1024,
        skipUTF8Validation: false,
        perMessageDeflate: true,
        followRedirects: false,
        maxRedirects: 10,
        ...options,
        socketPath: void 0,
        hostname: void 0,
        protocol: void 0,
        timeout: void 0,
        method: "GET",
        host: void 0,
        path: void 0,
        port: void 0
      };
      websocket._autoPong = opts.autoPong;
      websocket._closeTimeout = opts.closeTimeout;
      if (!protocolVersions.includes(opts.protocolVersion)) {
        throw new RangeError(
          `Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`
        );
      }
      let parsedUrl;
      if (address instanceof URL2) {
        parsedUrl = address;
      } else {
        try {
          parsedUrl = new URL2(address);
        } catch {
          throw new SyntaxError(`Invalid URL: ${address}`);
        }
      }
      if (parsedUrl.protocol === "http:") {
        parsedUrl.protocol = "ws:";
      } else if (parsedUrl.protocol === "https:") {
        parsedUrl.protocol = "wss:";
      }
      websocket._url = parsedUrl.href;
      const isSecure = parsedUrl.protocol === "wss:";
      const isIpcUrl = parsedUrl.protocol === "ws+unix:";
      let invalidUrlMessage;
      if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) {
        invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", "http:", "https:", or "ws+unix:"`;
      } else if (isIpcUrl && !parsedUrl.pathname) {
        invalidUrlMessage = "The URL's pathname is empty";
      } else if (parsedUrl.hash) {
        invalidUrlMessage = "The URL contains a fragment identifier";
      }
      if (invalidUrlMessage) {
        const err = new SyntaxError(invalidUrlMessage);
        if (websocket._redirects === 0) {
          throw err;
        } else {
          emitErrorAndClose(websocket, err);
          return;
        }
      }
      const defaultPort = isSecure ? 443 : 80;
      const key = randomBytes(16).toString("base64");
      const request = isSecure ? https.request : http2.request;
      const protocolSet = /* @__PURE__ */ new Set();
      let perMessageDeflate;
      opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
      opts.defaultPort = opts.defaultPort || defaultPort;
      opts.port = parsedUrl.port || defaultPort;
      opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
      opts.headers = {
        ...opts.headers,
        "Sec-WebSocket-Version": opts.protocolVersion,
        "Sec-WebSocket-Key": key,
        Connection: "Upgrade",
        Upgrade: "websocket"
      };
      opts.path = parsedUrl.pathname + parsedUrl.search;
      opts.timeout = opts.handshakeTimeout;
      if (opts.perMessageDeflate) {
        perMessageDeflate = new PerMessageDeflate({
          ...opts.perMessageDeflate,
          isServer: false,
          maxPayload: opts.maxPayload
        });
        opts.headers["Sec-WebSocket-Extensions"] = format({
          [PerMessageDeflate.extensionName]: perMessageDeflate.offer()
        });
      }
      if (protocols.length) {
        for (const protocol of protocols) {
          if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) {
            throw new SyntaxError(
              "An invalid or duplicated subprotocol was specified"
            );
          }
          protocolSet.add(protocol);
        }
        opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
      }
      if (opts.origin) {
        if (opts.protocolVersion < 13) {
          opts.headers["Sec-WebSocket-Origin"] = opts.origin;
        } else {
          opts.headers.Origin = opts.origin;
        }
      }
      if (parsedUrl.username || parsedUrl.password) {
        opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
      }
      if (isIpcUrl) {
        const parts = opts.path.split(":");
        opts.socketPath = parts[0];
        opts.path = parts[1];
      }
      let req;
      if (opts.followRedirects) {
        if (websocket._redirects === 0) {
          websocket._originalIpc = isIpcUrl;
          websocket._originalSecure = isSecure;
          websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
          const headers = options && options.headers;
          options = { ...options, headers: {} };
          if (headers) {
            for (const [key2, value] of Object.entries(headers)) {
              options.headers[key2.toLowerCase()] = value;
            }
          }
        } else if (websocket.listenerCount("redirect") === 0) {
          const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
          if (!isSameHost || websocket._originalSecure && !isSecure) {
            delete opts.headers.authorization;
            delete opts.headers.cookie;
            if (!isSameHost) delete opts.headers.host;
            opts.auth = void 0;
          }
        }
        if (opts.auth && !options.headers.authorization) {
          options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
        }
        req = websocket._req = request(opts);
        if (websocket._redirects) {
          websocket.emit("redirect", websocket.url, req);
        }
      } else {
        req = websocket._req = request(opts);
      }
      if (opts.timeout) {
        req.on("timeout", () => {
          abortHandshake(websocket, req, "Opening handshake has timed out");
        });
      }
      req.on("error", (err) => {
        if (req === null || req[kAborted]) return;
        req = websocket._req = null;
        emitErrorAndClose(websocket, err);
      });
      req.on("response", (res) => {
        const location = res.headers.location;
        const statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          let addr;
          try {
            addr = new URL2(location, address);
          } catch (e) {
            const err = new SyntaxError(`Invalid URL: ${location}`);
            emitErrorAndClose(websocket, err);
            return;
          }
          initAsClient(websocket, addr, protocols, options);
        } else if (!websocket.emit("unexpected-response", req, res)) {
          abortHandshake(
            websocket,
            req,
            `Unexpected server response: ${res.statusCode}`
          );
        }
      });
      req.on("upgrade", (res, socket, head) => {
        websocket.emit("upgrade", res);
        if (websocket.readyState !== WebSocket2.CONNECTING) return;
        req = websocket._req = null;
        const upgrade = res.headers.upgrade;
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshake(websocket, socket, "Invalid Upgrade header");
          return;
        }
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        const serverProt = res.headers["sec-websocket-protocol"];
        let protError;
        if (serverProt !== void 0) {
          if (!protocolSet.size) {
            protError = "Server sent a subprotocol but none was requested";
          } else if (!protocolSet.has(serverProt)) {
            protError = "Server sent an invalid subprotocol";
          }
        } else if (protocolSet.size) {
          protError = "Server sent no subprotocol";
        }
        if (protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        if (serverProt) websocket._protocol = serverProt;
        const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
        if (secWebSocketExtensions !== void 0) {
          if (!perMessageDeflate) {
            const message = "Server sent a Sec-WebSocket-Extensions header but no extension was requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          let extensions;
          try {
            extensions = parse(secWebSocketExtensions);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          const extensionNames = Object.keys(extensions);
          if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate.extensionName) {
            const message = "Server indicated an extension that was not requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          try {
            perMessageDeflate.accept(extensions[PerMessageDeflate.extensionName]);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          websocket._extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
        }
        websocket.setSocket(socket, head, {
          allowSynchronousEvents: opts.allowSynchronousEvents,
          generateMask: opts.generateMask,
          maxBufferedChunks: opts.maxBufferedChunks,
          maxFragments: opts.maxFragments,
          maxPayload: opts.maxPayload,
          skipUTF8Validation: opts.skipUTF8Validation
        });
      });
      if (opts.finishRequest) {
        opts.finishRequest(req, websocket);
      } else {
        req.end();
      }
    }
    function emitErrorAndClose(websocket, err) {
      websocket._readyState = WebSocket2.CLOSING;
      websocket._errorEmitted = true;
      websocket.emit("error", err);
      websocket.emitClose();
    }
    function netConnect(options) {
      options.path = options.socketPath;
      return net.connect(options);
    }
    function tlsConnect(options) {
      options.path = void 0;
      if (!options.servername && options.servername !== "") {
        options.servername = net.isIP(options.host) ? "" : options.host;
      }
      return tls.connect(options);
    }
    function abortHandshake(websocket, stream, message) {
      websocket._readyState = WebSocket2.CLOSING;
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshake);
      if (stream.setHeader) {
        stream[kAborted] = true;
        stream.abort();
        if (stream.socket && !stream.socket.destroyed) {
          stream.socket.destroy();
        }
        process.nextTick(emitErrorAndClose, websocket, err);
      } else {
        stream.destroy(err);
        stream.once("error", websocket.emit.bind(websocket, "error"));
        stream.once("close", websocket.emitClose.bind(websocket));
      }
    }
    function sendAfterClose(websocket, data, cb) {
      if (data) {
        const length = isBlob(data) ? data.size : toBuffer(data).length;
        if (websocket._socket) websocket._sender._bufferedBytes += length;
        else websocket._bufferedAmount += length;
      }
      if (cb) {
        const err = new Error(
          `WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`
        );
        process.nextTick(cb, err);
      }
    }
    function receiverOnConclude(code, reason) {
      const websocket = this[kWebSocket];
      websocket._closeFrameReceived = true;
      websocket._closeMessage = reason;
      websocket._closeCode = code;
      if (websocket._socket[kWebSocket] === void 0) return;
      websocket._socket.removeListener("data", socketOnData);
      process.nextTick(resume, websocket._socket);
      if (code === 1005) websocket.close();
      else websocket.close(code, reason);
    }
    function receiverOnDrain() {
      const websocket = this[kWebSocket];
      if (!websocket.isPaused) websocket._socket.resume();
    }
    function receiverOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket._socket[kWebSocket] !== void 0) {
        websocket._socket.removeListener("data", socketOnData);
        process.nextTick(resume, websocket._socket);
        websocket.close(err[kStatusCode]);
      }
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data, isBinary) {
      this[kWebSocket].emit("message", data, isBinary);
    }
    function receiverOnPing(data) {
      const websocket = this[kWebSocket];
      if (websocket._autoPong) websocket.pong(data, !this._isServer, NOOP);
      websocket.emit("ping", data);
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function resume(stream) {
      stream.resume();
    }
    function senderOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket.readyState === WebSocket2.CLOSED) return;
      if (websocket.readyState === WebSocket2.OPEN) {
        websocket._readyState = WebSocket2.CLOSING;
        setCloseTimer(websocket);
      }
      this._socket.end();
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function setCloseTimer(websocket) {
      websocket._closeTimer = setTimeout(
        websocket._socket.destroy.bind(websocket._socket),
        websocket._closeTimeout
      );
    }
    function socketOnClose() {
      const websocket = this[kWebSocket];
      this.removeListener("close", socketOnClose);
      this.removeListener("data", socketOnData);
      this.removeListener("end", socketOnEnd);
      websocket._readyState = WebSocket2.CLOSING;
      if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
        const chunk = this.read(this._readableState.length);
        websocket._receiver.write(chunk);
      }
      websocket._receiver.end();
      this[kWebSocket] = void 0;
      clearTimeout(websocket._closeTimer);
      if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
        websocket.emitClose();
      } else {
        websocket._receiver.on("error", receiverOnFinish);
        websocket._receiver.on("finish", receiverOnFinish);
      }
    }
    function socketOnData(chunk) {
      if (!this[kWebSocket]._receiver.write(chunk)) {
        this.pause();
      }
    }
    function socketOnEnd() {
      const websocket = this[kWebSocket];
      websocket._readyState = WebSocket2.CLOSING;
      websocket._receiver.end();
      this.end();
    }
    function socketOnError() {
      const websocket = this[kWebSocket];
      this.removeListener("error", socketOnError);
      this.on("error", NOOP);
      if (websocket) {
        websocket._readyState = WebSocket2.CLOSING;
        this.destroy();
      }
    }
  }
});

// node_modules/ws/lib/stream.js
var require_stream = __commonJS({
  "node_modules/ws/lib/stream.js"(exports2, module2) {
    "use strict";
    var WebSocket2 = require_websocket();
    var { Duplex } = require("stream");
    function emitClose(stream) {
      stream.emit("close");
    }
    function duplexOnEnd() {
      if (!this.destroyed && this._writableState.finished) {
        this.destroy();
      }
    }
    function duplexOnError(err) {
      this.removeListener("error", duplexOnError);
      this.destroy();
      if (this.listenerCount("error") === 0) {
        this.emit("error", err);
      }
    }
    function createWebSocketStream(ws, options) {
      let terminateOnDestroy = true;
      const duplex = new Duplex({
        ...options,
        autoDestroy: false,
        emitClose: false,
        objectMode: false,
        writableObjectMode: false
      });
      ws.on("message", function message(msg, isBinary) {
        const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
        if (!duplex.push(data)) ws.pause();
      });
      ws.once("error", function error(err) {
        if (duplex.destroyed) return;
        terminateOnDestroy = false;
        duplex.destroy(err);
      });
      ws.once("close", function close() {
        if (duplex.destroyed) return;
        duplex.push(null);
      });
      duplex._destroy = function(err, callback) {
        if (ws.readyState === ws.CLOSED) {
          callback(err);
          process.nextTick(emitClose, duplex);
          return;
        }
        let called = false;
        ws.once("error", function error(err2) {
          called = true;
          callback(err2);
        });
        ws.once("close", function close() {
          if (!called) callback(err);
          process.nextTick(emitClose, duplex);
        });
        if (terminateOnDestroy) ws.terminate();
      };
      duplex._final = function(callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._final(callback);
          });
          return;
        }
        if (ws._socket === null) return;
        if (ws._socket._writableState.finished) {
          callback();
          if (duplex._readableState.endEmitted) duplex.destroy();
        } else {
          ws._socket.once("finish", function finish() {
            callback();
          });
          ws.close();
        }
      };
      duplex._read = function() {
        if (ws.isPaused) ws.resume();
      };
      duplex._write = function(chunk, encoding, callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._write(chunk, encoding, callback);
          });
          return;
        }
        ws.send(chunk, callback);
      };
      duplex.on("end", duplexOnEnd);
      duplex.on("error", duplexOnError);
      return duplex;
    }
    module2.exports = createWebSocketStream;
  }
});

// node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS({
  "node_modules/ws/lib/subprotocol.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function parse(header) {
      const protocols = /* @__PURE__ */ new Set();
      let start = -1;
      let end = -1;
      let i = 0;
      for (i; i < header.length; i++) {
        const code = header.charCodeAt(i);
        if (end === -1 && tokenChars[code] === 1) {
          if (start === -1) start = i;
        } else if (i !== 0 && (code === 32 || code === 9)) {
          if (end === -1 && start !== -1) end = i;
        } else if (code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1) end = i;
          const protocol2 = header.slice(start, end);
          if (protocols.has(protocol2)) {
            throw new SyntaxError(`The "${protocol2}" subprotocol is duplicated`);
          }
          protocols.add(protocol2);
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      }
      if (start === -1 || end !== -1) {
        throw new SyntaxError("Unexpected end of input");
      }
      const protocol = header.slice(start, i);
      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }
      protocols.add(protocol);
      return protocols;
    }
    module2.exports = { parse };
  }
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "node_modules/ws/lib/websocket-server.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events");
    var http2 = require("http");
    var { Duplex } = require("stream");
    var { createHash } = require("crypto");
    var extension = require_extension();
    var PerMessageDeflate = require_permessage_deflate();
    var subprotocol = require_subprotocol();
    var WebSocket2 = require_websocket();
    var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants();
    var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
    var RUNNING = 0;
    var CLOSING = 1;
    var CLOSED = 2;
    var WebSocketServer = class extends EventEmitter {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Boolean} [options.autoPong=true] Specifies whether or not to
       *     automatically send a pong in response to a ping
       * @param {Number} [options.backlog=511] The maximum length of the queue of
       *     pending connections
       * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
       *     track clients
       * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
       *     wait for the closing handshake to finish after `websocket.close()` is
       *     called
       * @param {Function} [options.handleProtocols] A hook to handle protocols
       * @param {String} [options.host] The hostname where to bind the server
       * @param {Number} [options.maxBufferedChunks=262144] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=16384] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=104857600] The maximum allowed message
       *     size
       * @param {Boolean} [options.noServer=false] Enable no server mode
       * @param {String} [options.path] Accept only connections matching this path
       * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
       *     permessage-deflate
       * @param {Number} [options.port] The port where to bind the server
       * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
       *     server to use
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @param {Function} [options.verifyClient] A hook to reject connections
       * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
       *     class to use. It must be the `WebSocket` class or class that extends it
       * @param {Function} [callback] A listener for the `listening` event
       */
      constructor(options, callback) {
        super();
        options = {
          allowSynchronousEvents: true,
          autoPong: true,
          maxBufferedChunks: 256 * 1024,
          maxFragments: 16 * 1024,
          maxPayload: 100 * 1024 * 1024,
          skipUTF8Validation: false,
          perMessageDeflate: false,
          handleProtocols: null,
          clientTracking: true,
          closeTimeout: CLOSE_TIMEOUT,
          verifyClient: null,
          noServer: false,
          backlog: null,
          // use default (511 as implemented in net.js)
          server: null,
          host: null,
          path: null,
          port: null,
          WebSocket: WebSocket2,
          ...options
        };
        if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) {
          throw new TypeError(
            'One and only one of the "port", "server", or "noServer" options must be specified'
          );
        }
        if (options.port != null) {
          this._server = http2.createServer((req, res) => {
            const body = http2.STATUS_CODES[426];
            res.writeHead(426, {
              "Content-Length": body.length,
              "Content-Type": "text/plain"
            });
            res.end(body);
          });
          this._server.listen(
            options.port,
            options.host,
            options.backlog,
            callback
          );
        } else if (options.server) {
          this._server = options.server;
        }
        if (this._server) {
          const emitConnection = this.emit.bind(this, "connection");
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, emitConnection);
            }
          });
        }
        if (options.perMessageDeflate === true) options.perMessageDeflate = {};
        if (options.clientTracking) {
          this.clients = /* @__PURE__ */ new Set();
          this._shouldEmitClose = false;
        }
        this.options = options;
        this._state = RUNNING;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer) {
          throw new Error('The server is operating in "noServer" mode');
        }
        if (!this._server) return null;
        return this._server.address();
      }
      /**
       * Stop the server from accepting new connections and emit the `'close'` event
       * when all existing connections are closed.
       *
       * @param {Function} [cb] A one-time listener for the `'close'` event
       * @public
       */
      close(cb) {
        if (this._state === CLOSED) {
          if (cb) {
            this.once("close", () => {
              cb(new Error("The server is not running"));
            });
          }
          process.nextTick(emitClose, this);
          return;
        }
        if (cb) this.once("close", cb);
        if (this._state === CLOSING) return;
        this._state = CLOSING;
        if (this.options.noServer || this.options.server) {
          if (this._server) {
            this._removeListeners();
            this._removeListeners = this._server = null;
          }
          if (this.clients) {
            if (!this.clients.size) {
              process.nextTick(emitClose, this);
            } else {
              this._shouldEmitClose = true;
            }
          } else {
            process.nextTick(emitClose, this);
          }
        } else {
          const server = this._server;
          this._removeListeners();
          this._removeListeners = this._server = null;
          server.close(() => {
            emitClose(this);
          });
        }
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          const index = req.url.indexOf("?");
          const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
          if (pathname !== this.options.path) return false;
        }
        return true;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        const key = req.headers["sec-websocket-key"];
        const upgrade = req.headers.upgrade;
        const version = +req.headers["sec-websocket-version"];
        if (req.method !== "GET") {
          const message = "Invalid HTTP method";
          abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
          return;
        }
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          const message = "Invalid Upgrade header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (key === void 0 || !keyRegex.test(key)) {
          const message = "Missing or invalid Sec-WebSocket-Key header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (version !== 13 && version !== 8) {
          const message = "Missing or invalid Sec-WebSocket-Version header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
            "Sec-WebSocket-Version": "13, 8"
          });
          return;
        }
        if (!this.shouldHandle(req)) {
          abortHandshake(socket, 400);
          return;
        }
        const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
        let protocols = /* @__PURE__ */ new Set();
        if (secWebSocketProtocol !== void 0) {
          try {
            protocols = subprotocol.parse(secWebSocketProtocol);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Protocol header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
        const extensions = {};
        if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
          const perMessageDeflate = new PerMessageDeflate({
            ...this.options.perMessageDeflate,
            isServer: true,
            maxPayload: this.options.maxPayload
          });
          try {
            const offers = extension.parse(secWebSocketExtensions);
            if (offers[PerMessageDeflate.extensionName]) {
              perMessageDeflate.accept(offers[PerMessageDeflate.extensionName]);
              extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            const message = "Invalid or unacceptable Sec-WebSocket-Extensions header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        if (this.options.verifyClient) {
          const info = {
            origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
            secure: !!(req.socket.authorized || req.socket.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified) {
                return abortHandshake(socket, code || 401, message, headers);
              }
              this.completeUpgrade(
                extensions,
                key,
                protocols,
                req,
                socket,
                head,
                cb
              );
            });
            return;
          }
          if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
        }
        this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {Object} extensions The accepted extensions
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Set} protocols The subprotocols
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @throws {Error} If called more than once with the same socket
       * @private
       */
      completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
        if (!socket.readable || !socket.writable) return socket.destroy();
        if (socket[kWebSocket]) {
          throw new Error(
            "server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration"
          );
        }
        if (this._state > RUNNING) return abortHandshake(socket, 503);
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        const headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${digest}`
        ];
        const ws = new this.options.WebSocket(null, void 0, this.options);
        if (protocols.size) {
          const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
          if (protocol) {
            headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
            ws._protocol = protocol;
          }
        }
        if (extensions[PerMessageDeflate.extensionName]) {
          const params = extensions[PerMessageDeflate.extensionName].params;
          const value = extension.format({
            [PerMessageDeflate.extensionName]: [params]
          });
          headers.push(`Sec-WebSocket-Extensions: ${value}`);
          ws._extensions = extensions;
        }
        this.emit("headers", headers, req);
        socket.write(headers.concat("\r\n").join("\r\n"));
        socket.removeListener("error", socketOnError);
        ws.setSocket(socket, head, {
          allowSynchronousEvents: this.options.allowSynchronousEvents,
          maxBufferedChunks: this.options.maxBufferedChunks,
          maxFragments: this.options.maxFragments,
          maxPayload: this.options.maxPayload,
          skipUTF8Validation: this.options.skipUTF8Validation
        });
        if (this.clients) {
          this.clients.add(ws);
          ws.on("close", () => {
            this.clients.delete(ws);
            if (this._shouldEmitClose && !this.clients.size) {
              process.nextTick(emitClose, this);
            }
          });
        }
        cb(ws, req);
      }
    };
    module2.exports = WebSocketServer;
    function addListeners(server, map) {
      for (const event of Object.keys(map)) server.on(event, map[event]);
      return function removeListeners() {
        for (const event of Object.keys(map)) {
          server.removeListener(event, map[event]);
        }
      };
    }
    function emitClose(server) {
      server._state = CLOSED;
      server.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      message = message || http2.STATUS_CODES[code];
      headers = {
        Connection: "close",
        "Content-Type": "text/html",
        "Content-Length": Buffer.byteLength(message),
        ...headers
      };
      socket.once("finish", socket.destroy);
      socket.end(
        `HTTP/1.1 ${code} ${http2.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join("\r\n") + "\r\n\r\n" + message
      );
    }
    function abortHandshakeOrEmitwsClientError(server, req, socket, code, message, headers) {
      if (server.listenerCount("wsClientError")) {
        const err = new Error(message);
        Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
        server.emit("wsClientError", err, socket, req);
      } else {
        abortHandshake(socket, code, message, headers);
      }
    }
  }
});

// node_modules/ws/index.js
var require_ws = __commonJS({
  "node_modules/ws/index.js"(exports2, module2) {
    "use strict";
    var createWebSocketStream = require_stream();
    var extension = require_extension();
    var PerMessageDeflate = require_permessage_deflate();
    var Receiver = require_receiver();
    var Sender = require_sender();
    var subprotocol = require_subprotocol();
    var WebSocket2 = require_websocket();
    var WebSocketServer = require_websocket_server();
    WebSocket2.createWebSocketStream = createWebSocketStream;
    WebSocket2.extension = extension;
    WebSocket2.PerMessageDeflate = PerMessageDeflate;
    WebSocket2.Receiver = Receiver;
    WebSocket2.Sender = Sender;
    WebSocket2.Server = WebSocketServer;
    WebSocket2.subprotocol = subprotocol;
    WebSocket2.WebSocket = WebSocket2;
    WebSocket2.WebSocketServer = WebSocketServer;
    module2.exports = WebSocket2;
  }
});

// chatgpt_readiness.js
var require_chatgpt_readiness = __commonJS({
  "chatgpt_readiness.js"(exports2, module2) {
    var CHATGPT_READINESS_PROBES = Object.freeze([
      { name: "edge", url: "https://chatgpt.com/cdn-cgi/trace", kind: "edge" },
      { name: "session", url: "https://chatgpt.com/api/auth/session", kind: "api" },
      { name: "account", url: "https://chatgpt.com/backend-api/me", kind: "api" }
    ]);
    function normalizeProbeResult(probe, resource, elapsedMs) {
      const status = Number(resource?.httpStatusCode || 0);
      const netError = Number(resource?.netError || 0);
      const success = resource?.success === true || status > 0;
      return {
        name: probe.name,
        kind: probe.kind,
        url: probe.url,
        ok: success && netError === 0,
        status,
        elapsedMs,
        netError,
        error: resource?.netErrorName || (!success ? "no HTTP response" : null)
      };
    }
    function classifyChatGPTReadiness(results) {
      const completed = Array.isArray(results) ? results : [];
      const api = completed.filter((result) => result.kind === "api" && result.ok);
      const edge = completed.filter((result) => result.kind === "edge" && result.ok);
      const fastest = completed.filter((result) => result.ok).sort((left, right) => left.elapsedMs - right.elapsedMs)[0] || null;
      if (api.length) {
        return { state: "api-ready", ready: true, fastest, apiReady: true, edgeReady: !!edge.length };
      }
      if (edge.length) {
        return { state: "edge-only", ready: false, fastest, apiReady: false, edgeReady: true };
      }
      return { state: "blocked", ready: false, fastest: null, apiReady: false, edgeReady: false };
    }
    function selectReadinessRecovery2(readiness, genericInternetHealthy, alreadyReopenedPool = false) {
      if (readiness?.state === "api-ready") return "none";
      if (!genericInternetHealthy) return "wait-generic-route";
      if (!alreadyReopenedPool) return "reopen-connection-pool";
      return "reject-chatgpt-endpoint";
    }
    async function runChatGPTReadinessBenchmark2(cdp, options = {}) {
      const probes = options.probes || CHATGPT_READINESS_PROBES;
      const timeoutMs = options.timeoutMs || 15e3;
      const createTarget = options.createTarget !== false;
      const targets = [];
      const runProbe = async (probe) => {
        const startedAt2 = Date.now();
        let targetId = null;
        let sessionId = null;
        try {
          if (createTarget) {
            const target = await cdp.send("Target.createTarget", {
              url: "about:blank",
              background: true
            });
            targetId = target.targetId;
            targets.push(targetId);
            const attached = await cdp.send("Target.attachToTarget", {
              targetId,
              flatten: true
            });
            sessionId = attached.sessionId;
            await cdp.send("Network.enable", {}, sessionId);
            try {
              await cdp.send("Network.setBypassServiceWorker", { bypass: true }, sessionId);
            } catch {
            }
          }
          const response = await cdp.send("Network.loadNetworkResource", {
            url: `${probe.url}${probe.url.includes("?") ? "&" : "?"}readiness=${Date.now()}`,
            options: {
              disableCache: true,
              includeCredentials: true
            }
          }, sessionId, timeoutMs);
          return normalizeProbeResult(probe, response?.resource, Date.now() - startedAt2);
        } catch (error) {
          return {
            name: probe.name,
            kind: probe.kind,
            url: probe.url,
            ok: false,
            status: 0,
            elapsedMs: Date.now() - startedAt2,
            netError: 0,
            error: error.message
          };
        }
      };
      const results = await Promise.all(probes.map(runProbe));
      await Promise.all(targets.map(
        (targetId) => cdp.send("Target.closeTarget", { targetId }).catch(() => null)
      ));
      return {
        ...classifyChatGPTReadiness(results),
        elapsedMs: Math.max(0, ...results.map((result) => result.elapsedMs)),
        results
      };
    }
    module2.exports = {
      CHATGPT_READINESS_PROBES,
      normalizeProbeResult,
      classifyChatGPTReadiness,
      selectReadinessRecovery: selectReadinessRecovery2,
      runChatGPTReadinessBenchmark: runChatGPTReadinessBenchmark2
    };
  }
});

// recovery_race.js
var require_recovery_race = __commonJS({
  "recovery_race.js"(exports2, module2) {
    "use strict";
    var defaultNow = () => Date.now();
    async function raceRecoveryStrategies({
      strategies,
      validate,
      expectedCountry,
      timeoutMs = 9e4,
      trace = () => {
      },
      now = defaultNow
    }) {
      if (!Array.isArray(strategies) || strategies.length === 0) {
        throw new Error("At least one recovery strategy is required");
      }
      const names = strategies.map((strategy) => strategy.name);
      if (names.some((name) => !name) || new Set(names).size !== names.length) {
        throw new Error("Recovery strategies require unique non-empty names");
      }
      const startedAt2 = now();
      const controllers = /* @__PURE__ */ new Map();
      let settled = false;
      let remaining = strategies.length;
      const failures = [];
      trace("recovery_race_started", {
        expectedCountry,
        timeoutMs,
        strategies: strategies.map((strategy) => strategy.name)
      });
      return new Promise((resolve, reject) => {
        const finishFailure = (error) => {
          if (settled) return;
          settled = true;
          clearTimeout(deadline);
          for (const controller of controllers.values()) controller.abort("race-finished");
          trace("recovery_race_failed", {
            expectedCountry,
            elapsedMs: now() - startedAt2,
            failures
          });
          reject(error);
        };
        const deadline = setTimeout(() => {
          failures.push({ strategy: "race", reason: `timeout after ${timeoutMs}ms` });
          finishFailure(new Error(`Recovery race timed out after ${timeoutMs}ms`));
        }, timeoutMs);
        for (const strategy of strategies) {
          const controller = new AbortController();
          controllers.set(strategy.name, controller);
          const strategyStartedAt = now();
          trace("recovery_strategy_started", {
            strategy: strategy.name,
            expectedCountry
          });
          Promise.resolve().then(() => strategy.run({
            signal: controller.signal,
            expectedCountry,
            strategy: strategy.name
          })).then(async (candidate) => {
            if (settled || controller.signal.aborted) return;
            const verdict = await validate(candidate, {
              expectedCountry,
              strategy: strategy.name,
              signal: controller.signal
            });
            if (settled || controller.signal.aborted) return;
            if (!verdict?.ok) {
              const reason = verdict?.reason || "candidate did not verify";
              failures.push({ strategy: strategy.name, reason });
              trace("recovery_strategy_rejected", {
                strategy: strategy.name,
                elapsedMs: now() - strategyStartedAt,
                reason,
                country: candidate?.verification?.state?.country || candidate?.state?.country || candidate?.country || null
              });
              return;
            }
            settled = true;
            clearTimeout(deadline);
            for (const [name, loser] of controllers) {
              if (name !== strategy.name) {
                loser.abort(`winner:${strategy.name}`);
                trace("recovery_strategy_cancelled", {
                  strategy: name,
                  winner: strategy.name,
                  reason: `winner:${strategy.name}`
                });
              }
            }
            const result = {
              strategy: strategy.name,
              candidate,
              verification: verdict,
              elapsedMs: now() - startedAt2
            };
            trace("recovery_race_winner", {
              strategy: strategy.name,
              expectedCountry,
              elapsedMs: result.elapsedMs,
              country: verdict.country || candidate?.verification?.state?.country || candidate?.state?.country || candidate?.country || null
            });
            resolve(result);
          }).catch((error) => {
            if (settled) return;
            const cancelled = controller.signal.aborted;
            if (!cancelled) {
              failures.push({ strategy: strategy.name, reason: error?.message || String(error) });
            }
            trace(cancelled ? "recovery_strategy_cancelled" : "recovery_strategy_failed", {
              strategy: strategy.name,
              elapsedMs: now() - strategyStartedAt,
              reason: cancelled ? controller.signal.reason : error?.message || String(error)
            });
          }).finally(() => {
            remaining--;
            if (!settled && remaining === 0) {
              finishFailure(new Error(`No recovery strategy verified ${expectedCountry || "the route"}`));
            }
          });
        }
      });
    }
    module2.exports = { raceRecoveryStrategies };
  }
});

// isolated_chatgpt_recovery.js
var require_isolated_chatgpt_recovery = __commonJS({
  "isolated_chatgpt_recovery.js"(exports2, module2) {
    "use strict";
    var { raceRecoveryStrategies } = require_recovery_race();
    var sleep2 = (ms, signal) => new Promise((resolve, reject) => {
      if (signal?.aborted) return reject(new Error(`cancelled: ${signal.reason || "aborted"}`));
      const timer = setTimeout(resolve, ms);
      signal?.addEventListener("abort", () => {
        clearTimeout(timer);
        reject(new Error(`cancelled: ${signal.reason || "aborted"}`));
      }, { once: true });
    });
    var marker = (raceId, strategy) => `recovery_race=${encodeURIComponent(raceId)}&strategy=${encodeURIComponent(strategy)}`;
    async function closeTarget(cdp, targetId) {
      if (!targetId) return;
      try {
        await cdp.send("Target.closeTarget", { targetId });
      } catch {
      }
    }
    async function attachBackgroundTarget(cdp, { initialUrl = "about:blank", signal }) {
      const target = await cdp.send("Target.createTarget", {
        url: initialUrl,
        background: true
      });
      const targetId = target.targetId;
      let closed = false;
      const close = async () => {
        if (closed) return;
        closed = true;
        await closeTarget(cdp, targetId);
      };
      signal.addEventListener("abort", () => {
        void close();
      }, { once: true });
      if (signal.aborted) {
        await close();
        throw new Error(`cancelled: ${signal.reason || "aborted"}`);
      }
      try {
        const attached = await cdp.send("Target.attachToTarget", {
          targetId,
          flatten: true
        });
        const sessionId = attached.sessionId;
        await Promise.all([
          cdp.send("Network.enable", {}, sessionId),
          cdp.send("Page.enable", {}, sessionId),
          cdp.send("Runtime.enable", {}, sessionId)
        ]);
        try {
          await cdp.send("Network.setBypassServiceWorker", { bypass: true }, sessionId);
        } catch {
        }
        return { targetId, sessionId, close };
      } catch (error) {
        await close();
        throw error;
      }
    }
    async function navigate(cdp, sessionId, url, signal) {
      if (signal.aborted) throw new Error(`cancelled: ${signal.reason || "aborted"}`);
      const result = await cdp.send("Page.navigate", { url }, sessionId, 3e4);
      if (result?.errorText) throw new Error(result.errorText);
    }
    async function waitForChatGPTContext(cdp, sessionId, signal, timeoutMs) {
      const deadline = Date.now() + timeoutMs;
      let last = {};
      while (Date.now() < deadline) {
        if (signal.aborted) throw new Error(`cancelled: ${signal.reason || "aborted"}`);
        try {
          const evaluated = await cdp.send("Runtime.evaluate", {
            expression: `JSON.stringify({
          host: location.hostname,
          protocol: location.protocol,
          ready: document.readyState,
          hasBody: !!document.body
        })`,
            returnByValue: true,
            timeout: 2e3
          }, sessionId, 3e3);
          last = JSON.parse(evaluated.result?.value || "{}");
          if (last.host === "chatgpt.com" && last.hasBody && (last.ready === "interactive" || last.ready === "complete")) return last;
          if (/^chrome-error:/i.test(last.protocol || "")) {
            throw new Error("ChatGPT navigation reached a Chrome error page");
          }
        } catch (error) {
          if (/Chrome error page/.test(error.message)) throw error;
        }
        await sleep2(150, signal);
      }
      throw new Error(`ChatGPT context did not become ready in ${timeoutMs}ms`);
    }
    async function probeAuthenticatedCountry(cdp, sessionId, signal) {
      if (signal.aborted) throw new Error(`cancelled: ${signal.reason || "aborted"}`);
      const evaluated = await cdp.send("Runtime.evaluate", {
        expression: `(async () => {
      const read = async url => {
        try {
          const response = await fetch(url, {credentials:'include', cache:'no-store'});
          const text = await response.text();
          let json = null;
          try { json = JSON.parse(text); } catch {}
          return {status:response.status, json, html:json === null && /^\\s*</.test(text)};
        } catch (error) {
          return {status:0, error:error.message};
        }
      };
      const [session, me] = await Promise.all([
        read('/api/auth/session'),
        read('/backend-api/me')
      ]);
      return JSON.stringify({
        loggedIn: !!session.json?.user,
        email: session.json?.user?.email || null,
        sessionStatus: session.status,
        status: me.status,
        country: me.json?.country || null,
        challenge: session.html || me.html,
        error: session.error || me.error || null
      });
    })()`,
        awaitPromise: true,
        returnByValue: true,
        timeout: 2e4
      }, sessionId, 25e3);
      return JSON.parse(evaluated.result?.value || "{}");
    }
    async function verifyAuthenticatedCountry(cdp, sessionId, expectedCountry, signal, settleMs) {
      const first = await probeAuthenticatedCountry(cdp, sessionId, signal);
      if (!first.loggedIn || first.sessionStatus !== 200 || first.status !== 200 || first.country !== expectedCountry) {
        return { ok: false, state: first, samples: 1 };
      }
      await sleep2(settleMs, signal);
      const second = await probeAuthenticatedCountry(cdp, sessionId, signal);
      const ok = second.loggedIn && second.sessionStatus === 200 && second.status === 200 && second.country === expectedCountry && second.country === first.country;
      return { ok, state: second, first, samples: 2 };
    }
    function strategyDefinitions(raceId) {
      const query = (strategy) => marker(raceId, strategy);
      return [
        {
          name: "root-document",
          initialUrl: "about:blank",
          steps: [
            { type: "navigate", url: `https://chatgpt.com/?${query("root-document")}` }
          ]
        },
        {
          name: "auth-refresh-document",
          initialUrl: "about:blank",
          steps: [
            {
              type: "navigate",
              url: `https://chatgpt.com/api/auth/session?refresh=true&reason=integrity_state_mismatch&${query("auth-refresh-document")}`
            },
            { type: "navigate", url: `https://chatgpt.com/?${query("auth-refresh-document")}` }
          ]
        },
        {
          name: "account-endpoint-bootstrap",
          // Starting at createTarget exercises Chromium's browser-level navigation
          // path instead of issuing Page.navigate for the first request.
          initialUrl: `https://chatgpt.com/backend-api/me?${query("account-endpoint-bootstrap")}`,
          waitForInitial: true,
          steps: [
            { type: "navigate", url: `https://chatgpt.com/?${query("account-endpoint-bootstrap")}` }
          ]
        }
      ];
    }
    async function runIsolatedChatGPTRecovery2(cdp, {
      expectedCountry,
      timeoutMs = 9e4,
      targetReadyTimeoutMs = 3e4,
      verificationSettleMs = 900,
      trace = () => {
      },
      raceId = `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
      definitions = strategyDefinitions(raceId)
    }) {
      if (!/^[A-Z]{2}$/.test(expectedCountry || "")) {
        throw new Error("expectedCountry must be an uppercase ISO-3166 alpha-2 code");
      }
      const strategies = definitions.map((definition) => ({
        name: definition.name,
        run: async ({ signal }) => {
          let target;
          const startedAt2 = Date.now();
          try {
            target = await attachBackgroundTarget(cdp, {
              initialUrl: definition.initialUrl,
              signal
            });
            if (definition.waitForInitial) {
              await waitForChatGPTContext(
                cdp,
                target.sessionId,
                signal,
                targetReadyTimeoutMs
              );
            }
            for (const step of definition.steps) {
              await navigate(cdp, target.sessionId, step.url, signal);
              await waitForChatGPTContext(
                cdp,
                target.sessionId,
                signal,
                targetReadyTimeoutMs
              );
            }
            await waitForChatGPTContext(cdp, target.sessionId, signal, targetReadyTimeoutMs);
            const verification = await verifyAuthenticatedCountry(
              cdp,
              target.sessionId,
              expectedCountry,
              signal,
              verificationSettleMs
            );
            return {
              ready: true,
              targetId: target.targetId,
              verification,
              elapsedMs: Date.now() - startedAt2
            };
          } finally {
            if (target) await target.close();
          }
        }
      }));
      return raceRecoveryStrategies({
        strategies,
        expectedCountry,
        timeoutMs,
        trace,
        validate: async (candidate) => {
          const verification = candidate?.verification;
          const state = verification?.state || {};
          const ok = candidate?.ready && verification?.ok && verification.samples === 2 && state.loggedIn && state.sessionStatus === 200 && state.status === 200 && state.country === expectedCountry;
          return {
            ok,
            country: state.country || null,
            reason: ok ? null : `verification failed (session=${state.sessionStatus || 0}, me=${state.status || 0}, country=${state.country || "?"})`
          };
        }
      });
    }
    module2.exports = {
      runIsolatedChatGPTRecovery: runIsolatedChatGPTRecovery2,
      strategyDefinitions,
      verifyAuthenticatedCountry
    };
  }
});

// isolated_checkout.js
var require_isolated_checkout = __commonJS({
  "isolated_checkout.js"(exports2, module2) {
    var crypto2 = require("crypto");
    var CHATGPT_ORIGIN = "https://chatgpt.com";
    var COUNTRY_PROVIDERS = [
      { name: "api.country.is", url: "https://api.country.is/", field: "country" },
      { name: "ipwho.is", url: "https://ipwho.is/", field: "country_code" },
      { name: "ipinfo.io", url: "https://ipinfo.io/json", field: "country" }
    ];
    var fingerprint2 = (value) => value ? crypto2.createHash("sha256").update(String(value)).digest("hex").slice(0, 8) : "none";
    function abortError(reason = "Checkout cancelled") {
      const error = new Error(reason);
      error.name = "AbortError";
      return error;
    }
    var ProgressBudget = class {
      constructor({ signal, trace, idleTimeoutMs = 3e4, hardTimeoutMs = 18e4 }) {
        this.signal = signal;
        this.trace = trace;
        this.idleTimeoutMs = idleTimeoutMs;
        this.hardDeadline = Date.now() + hardTimeoutMs;
        this.lastProgressAt = Date.now();
        this.pausedAt = null;
      }
      check() {
        if (this.signal?.aborted) throw abortError(this.signal.reason?.message || "Checkout cancelled");
        if (Date.now() >= this.hardDeadline) throw new Error("Checkout hard timeout exceeded");
        if (Date.now() - this.lastProgressAt >= this.idleTimeoutMs) {
          throw new Error("Checkout stopped making progress");
        }
      }
      progress(stage, details = {}) {
        this.check();
        this.lastProgressAt = Date.now();
        this.trace(stage, details);
      }
      touch() {
        if (!this.pausedAt) this.lastProgressAt = Date.now();
      }
      pause() {
        if (!this.pausedAt) this.pausedAt = Date.now();
      }
      resume() {
        if (this.pausedAt) {
          this.hardDeadline += Date.now() - this.pausedAt;
          this.pausedAt = null;
        }
        this.lastProgressAt = Date.now();
        if (this.signal?.aborted) throw abortError(this.signal.reason?.message || "Checkout cancelled");
      }
      async run(stage, timeoutMs, operation) {
        this.progress(`${stage}_started`);
        const stageDeadline = Date.now() + timeoutMs;
        let monitorTimer;
        try {
          const monitor = new Promise((_, reject) => {
            monitorTimer = setInterval(() => {
              const now = Date.now();
              if (this.signal?.aborted) {
                reject(abortError(this.signal.reason?.message || "Checkout cancelled"));
              } else if (now >= this.hardDeadline) {
                reject(new Error("Checkout hard timeout exceeded"));
              } else if (now >= stageDeadline) {
                reject(new Error(`${stage} timed out after ${timeoutMs}ms`));
              } else if (now - this.lastProgressAt >= this.idleTimeoutMs) {
                reject(new Error(`${stage} stopped making progress for ${this.idleTimeoutMs}ms`));
              }
            }, Math.max(5, Math.min(250, Math.floor(this.idleTimeoutMs / 4))));
          });
          const result = await Promise.race([operation(), monitor]);
          this.progress(`${stage}_succeeded`);
          return result;
        } catch (error) {
          this.trace(`${stage}_failed`, { error: error.message });
          throw error;
        } finally {
          clearInterval(monitorTimer);
        }
      }
    };
    function normalizeAuth(apiSession) {
      const account = typeof apiSession?.account === "string" ? apiSession.account : apiSession?.account?.id || apiSession?.account?.account_id || null;
      return {
        token: typeof apiSession?.accessToken === "string" ? apiSession.accessToken : null,
        account
      };
    }
    async function createApiTarget(cdp, budget) {
      const target = await budget.run("api_target_create", 1e4, () => cdp.send("Target.createTarget", { url: "about:blank", background: true }, null, 1e4));
      const targetId = target?.targetId;
      if (!targetId) throw new Error("CDP did not return an API target id");
      try {
        const attached = await budget.run("api_target_attach", 1e4, () => cdp.send("Target.attachToTarget", { targetId, flatten: true }, null, 1e4));
        if (!attached?.sessionId) throw new Error("CDP did not attach the API target");
        const sessionId = attached.sessionId;
        await Promise.all([
          cdp.send("Runtime.enable", {}, sessionId, 1e4),
          cdp.send("Page.enable", {}, sessionId, 1e4),
          cdp.send("Network.enable", {}, sessionId, 1e4)
        ]);
        try {
          await cdp.send("Network.setBypassServiceWorker", { bypass: true }, sessionId, 5e3);
        } catch {
        }
        budget.progress("api_target_ready", { targetId, sessionId });
        return { targetId, sessionId };
      } catch (error) {
        try {
          await cdp.send("Target.closeTarget", { targetId }, null, 5e3);
        } catch {
        }
        throw error;
      }
    }
    async function evaluateJson(cdp, sessionId, expression, timeoutMs) {
      const response = await cdp.send("Runtime.evaluate", {
        expression,
        awaitPromise: true,
        returnByValue: true,
        timeout: timeoutMs
      }, sessionId, timeoutMs + 5e3);
      const value = response?.result?.value;
      if (typeof value !== "string") throw new Error("API target returned no result");
      return JSON.parse(value);
    }
    async function probeCountry(cdp, sessionId, budget, provider, timeoutMs = 8e3) {
      const expression = `(async () => {
    /* isolated:country:${provider.name} */
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ${timeoutMs});
    try {
      const response = await fetch(${JSON.stringify(provider.url)} +
        (${JSON.stringify(provider.url)}.includes('?') ? '&' : '?') + '_=' + Date.now(), {
        cache: 'no-store',
        signal: controller.signal
      });
      const data = await response.json();
      const country = String(data[${JSON.stringify(provider.field)}] || '').toUpperCase();
      return JSON.stringify({
        ok: response.ok && /^[A-Z]{2}$/.test(country),
        country,
        status: response.status
      });
    } catch (error) {
      return JSON.stringify({ok:false, error:error.name === 'AbortError' ? 'provider timeout' : error.message});
    } finally {
      clearTimeout(timer);
    }
  })()`;
      return budget.run(
        `country_probe_${provider.name.replace(/\W/g, "_")}`,
        timeoutMs + 7e3,
        () => evaluateJson(cdp, sessionId, expression, timeoutMs + 2e3)
      );
    }
    async function detectExitCountry(cdp, sessionId, budget) {
      const observations = [];
      for (const provider of COUNTRY_PROVIDERS) {
        budget.check();
        try {
          const result = await probeCountry(cdp, sessionId, budget, provider);
          observations.push({
            provider: provider.name,
            ok: !!result.ok,
            country: /^[A-Z]{2}$/.test(result.country || "") ? result.country : null
          });
        } catch (error) {
          observations.push({ provider: provider.name, ok: false, country: null, error: error.message });
        }
      }
      const counts = /* @__PURE__ */ new Map();
      for (const item of observations.filter((item2) => item2.ok && item2.country)) {
        counts.set(item.country, (counts.get(item.country) || 0) + 1);
      }
      const ranked = [...counts.entries()].sort((a, b) => b[1] - a[1]);
      const country = ranked[0]?.[0] || null;
      const agreeingProviders = ranked[0]?.[1] || 0;
      const reliable = agreeingProviders >= 2;
      budget.progress("exit_country_observed", {
        country,
        reliable,
        agreeingProviders,
        observations
      });
      return { country, reliable, agreeingProviders, observations };
    }
    async function establishApiOrigin(cdp, sessionId, budget) {
      const marker = `isolated_checkout=${Date.now()}`;
      await budget.run("api_origin_navigation", 3e4, async () => {
        const result = await cdp.send("Page.navigate", {
          url: `${CHATGPT_ORIGIN}/api/auth/session?${marker}`
        }, sessionId, 25e3);
        if (result?.errorText) throw new Error(`API origin navigation failed: ${result.errorText}`);
        const deadline = Date.now() + 2e4;
        while (Date.now() < deadline) {
          budget.check();
          try {
            const state = await evaluateJson(
              cdp,
              sessionId,
              `JSON.stringify({host:location.hostname,ready:document.readyState})`,
              3e3
            );
            if (state.host === "chatgpt.com" && /interactive|complete/.test(state.ready || "")) return;
          } catch {
          }
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
        throw new Error("The isolated API target could not reach chatgpt.com");
      });
    }
    async function resolveAuth(cdp, sessionId, apiSession, budget) {
      const supplied = normalizeAuth(apiSession);
      if (supplied.token) {
        budget.progress("api_auth_ready", {
          source: "session_api.json",
          tokenFingerprint: fingerprint2(supplied.token),
          accountKnown: !!supplied.account
        });
        return supplied;
      }
      const expression = `(async () => {
    /* isolated:session */
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch('/api/auth/session', {
        credentials:'include', cache:'no-store', signal:controller.signal
      });
      const data = await response.json();
      const account = typeof data.account === 'string'
        ? data.account : (data.account?.id || data.account?.account_id || null);
      return JSON.stringify({status:response.status, token:data.accessToken || null, account});
    } catch (error) {
      return JSON.stringify({status:0,error:error.message});
    } finally { clearTimeout(timer); }
  })()`;
      const result = await budget.run(
        "api_auth_session",
        22e3,
        () => evaluateJson(cdp, sessionId, expression, 18e3)
      );
      if (!result.token) throw new Error(`No access token available in isolated API target (HTTP ${result.status || 0})`);
      budget.progress("api_auth_ready", {
        source: "isolated session endpoint",
        tokenFingerprint: fingerprint2(result.token),
        accountKnown: !!result.account
      });
      return { token: result.token, account: result.account };
    }
    async function requestSentinel(cdp, sessionId, auth, budget) {
      const expression = `(async () => {
    /* isolated:sentinel */
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12000);
    try {
      const headers = {
        'Content-Type':'application/json',
        'Authorization':'Bearer ' + ${JSON.stringify(auth.token)},
        'OAI-Language':'en-US'
      };
      if (${JSON.stringify(auth.account)}) headers['ChatGPT-Account-ID'] = ${JSON.stringify(auth.account)};
      const response = await fetch('/backend-api/sentinel/chat-requirements', {
        method:'POST', headers, body:'{}', credentials:'include', signal:controller.signal
      });
      const data = response.ok ? await response.json().catch(() => ({})) : {};
      return JSON.stringify({status:response.status, token:data.token || null});
    } catch (error) {
      return JSON.stringify({status:0,error:error.name === 'AbortError' ? 'sentinel timeout' : error.message});
    } finally { clearTimeout(timer); }
  })()`;
      try {
        return await budget.run(
          "sentinel_request",
          18e3,
          () => evaluateJson(cdp, sessionId, expression, 15e3)
        );
      } catch (error) {
        return { status: 0, token: null, error: error.message };
      }
    }
    async function requestCheckout(cdp, sessionId, auth, sentinel, country, currency, budget) {
      const body = {
        entry_point: "all_plans_pricing_modal",
        plan_name: "chatgptplusplan",
        billing_details: { country, currency },
        checkout_ui_mode: "custom"
      };
      const expression = `(async () => {
    /* isolated:checkout */
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 60000);
    try {
      const headers = {
        'Content-Type':'application/json',
        'Authorization':'Bearer ' + ${JSON.stringify(auth.token)},
        'OAI-Language':'en-US'
      };
      if (${JSON.stringify(auth.account)}) headers['ChatGPT-Account-ID'] = ${JSON.stringify(auth.account)};
      if (${JSON.stringify(sentinel.token || null)}) {
        headers['openai-sentinel-chat-requirements-token'] = ${JSON.stringify(sentinel.token || null)};
      }
      const response = await fetch('/backend-api/payments/checkout', {
        method:'POST',
        headers,
        body:${JSON.stringify(JSON.stringify(body))},
        credentials:'include',
        signal:controller.signal
      });
      const text = await response.text();
      return JSON.stringify({ok:response.ok,status:response.status,body:text});
    } catch (error) {
      return JSON.stringify({ok:false,status:0,error:error.name === 'AbortError' ? 'checkout timeout' : error.message});
    } finally { clearTimeout(timer); }
  })()`;
      const response = await budget.run(
        "checkout_request",
        7e4,
        () => evaluateJson(cdp, sessionId, expression, 65e3)
      );
      if (!response.ok) {
        const detail = response.error || String(response.body || "").slice(0, 300);
        throw new Error(`Checkout HTTP ${response.status || 0}: ${detail}`);
      }
      let data;
      try {
        data = JSON.parse(response.body);
      } catch {
        throw new Error("Checkout returned invalid JSON");
      }
      if (!data.checkout_session_id) throw new Error("Checkout response had no checkout_session_id");
      return {
        url: `${CHATGPT_ORIGIN}/checkout/${data.processor_entity || "openai_llc"}/${data.checkout_session_id}`,
        country: data.billing_details?.country || country,
        currency: data.billing_details?.currency || currency,
        sentinelUsed: !!sentinel.token
      };
    }
    async function runIsolatedCheckout2(options) {
      const {
        cdp,
        apiSession,
        currencyForCountry: currencyForCountry2,
        confirm,
        signal,
        trace: rawTrace = () => {
        },
        idleTimeoutMs,
        hardTimeoutMs
      } = options;
      if (!cdp || typeof cdp.send !== "function") throw new Error("A CDP client is required");
      if (typeof confirm !== "function") throw new Error("A checkout confirmation callback is required");
      const trace = (stage, details = {}) => rawTrace("checkout_api_stage", { stage, ...details });
      const budget = new ProgressBudget({ signal, trace, idleTimeoutMs, hardTimeoutMs });
      let target;
      let stopNetworkProgress = null;
      try {
        target = await createApiTarget(cdp, budget);
        if (typeof cdp.on === "function") {
          stopNetworkProgress = cdp.on("Network.responseReceived", (_params, eventSessionId) => {
            if (eventSessionId === target.sessionId) budget.touch();
          });
        }
        const detected = await detectExitCountry(cdp, target.sessionId, budget);
        await establishApiOrigin(cdp, target.sessionId, budget);
        const auth = await resolveAuth(cdp, target.sessionId, apiSession, budget);
        budget.progress("user_confirmation_requested", {
          country: detected.country,
          reliable: detected.reliable,
          agreeingProviders: detected.agreeingProviders
        });
        budget.pause();
        let decision;
        try {
          decision = await confirm(detected);
        } finally {
          budget.resume();
        }
        if (!decision?.confirmed) {
          budget.progress("user_confirmation_declined");
          return { cancelled: true };
        }
        const country = String(decision.country || detected.country || "").toUpperCase();
        if (!/^[A-Z]{2}$/.test(country)) throw new Error("A confirmed 2-letter billing country is required");
        const currency = currencyForCountry2(country);
        budget.progress("user_confirmation_received", { country, currency });
        const sentinel = await requestSentinel(cdp, target.sessionId, auth, budget);
        budget.progress("sentinel_result", { status: sentinel.status || 0, tokenReceived: !!sentinel.token });
        const result = await requestCheckout(
          cdp,
          target.sessionId,
          auth,
          sentinel,
          country,
          currency,
          budget
        );
        budget.progress("checkout_ready", {
          country: result.country,
          currency: result.currency,
          sentinelUsed: result.sentinelUsed
        });
        return result;
      } finally {
        if (typeof stopNetworkProgress === "function") stopNetworkProgress();
        if (target?.targetId) {
          try {
            await cdp.send("Target.closeTarget", { targetId: target.targetId }, null, 5e3);
            trace("api_target_closed", { targetId: target.targetId });
          } catch (error) {
            trace("api_target_close_failed", { error: error.message });
          }
        }
      }
    }
    module2.exports = {
      COUNTRY_PROVIDERS,
      ProgressBudget,
      normalizeAuth,
      runIsolatedCheckout: runIsolatedCheckout2
    };
  }
});

// manual_browser_smart (17).js
var { spawn } = require("child_process");
var http = require("http");
var WebSocket = require_ws();
var fs = require("fs");
var path = require("path");
var os = require("os");
var readline = require("readline");
var crypto = require("crypto");
var {
  runChatGPTReadinessBenchmark,
  selectReadinessRecovery
} = require_chatgpt_readiness();
var { runIsolatedChatGPTRecovery } = require_isolated_chatgpt_recovery();
var { runIsolatedCheckout } = require_isolated_checkout();
var CLI_ARGS = process.argv.slice(2);
var cliFlag = (name) => CLI_ARGS.includes(`--${name}`);
var cliValue = (name, fallback = null) => {
  const inline = CLI_ARGS.find((arg) => arg.startsWith(`--${name}=`));
  if (inline) return inline.slice(name.length + 3);
  const index = CLI_ARGS.indexOf(`--${name}`);
  if (index >= 0 && CLI_ARGS[index + 1] && !CLI_ARGS[index + 1].startsWith("--")) return CLI_ARGS[index + 1];
  return fallback;
};
var USE_TEMP_PROFILE = cliFlag("temp-profile");
var PROFILE_DIR = path.resolve(cliValue("profile", "./chatgpt_profile"));
var TRACE_ENABLED = process.env.TRACE !== "0";
var TRACE_ASSETS = process.env.TRACE_ASSETS === "1";
var TRACE_TELEMETRY = process.env.TRACE_TELEMETRY === "1";
var TELEMETRY_PATTERN = /chatgpt\.com\/(ces\/|backend-api\/(?:lat|edge)\/)|\/telemetry\/intake|cdn-cgi\/challenge-platform/i;
var isTelemetryUrl = (url) => !TRACE_TELEMETRY && TELEMETRY_PATTERN.test(String(url || ""));
var ASSET_TYPES = /* @__PURE__ */ new Set(["Image", "Font", "Stylesheet", "Media", "Manifest", "Script", "Prefetch"]);
var SECRET_HEADERS = /* @__PURE__ */ new Set(["cookie", "set-cookie", "authorization", "proxy-authorization"]);
var startedAt = Date.now();
var fingerprint = (value) => value ? crypto.createHash("sha256").update(String(value)).digest("hex").slice(0, 8) : "none";
var elapsedStamp = () => `${((Date.now() - startedAt) / 1e3).toFixed(3).padStart(9)}s`;
var shortenUrl = (url, max = 96) => {
  try {
    const parsed = new URL(url);
    const tail = parsed.pathname + parsed.search.slice(0, 40);
    const text = parsed.hostname + tail;
    return text.length > max ? text.slice(0, max) + "\u2026" : text;
  } catch {
    return String(url).slice(0, max);
  }
};
var redactHeaders = (headers) => Object.fromEntries(
  Object.entries(headers || {}).map(([name, value]) => SECRET_HEADERS.has(name.toLowerCase()) ? [name, `<redacted ${String(value).length}b fp=${fingerprint(value)}>`] : [name, String(value).slice(0, 200)])
);
var sleep = (ms) => new Promise((r) => setTimeout(r, ms));
process.on("uncaughtException", (e) => {
  console.error(`
\u274C Unexpected error: ${e?.stack || e}`);
});
process.on("unhandledRejection", (e) => {
  console.error(`
\u274C Unhandled promise rejection: ${e?.stack || e}`);
});
var COOKIE_NAME = "__Secure-next-auth.session-token";
var COUNTRY_CURRENCY = {
  US: "USD",
  CA: "CAD",
  MX: "MXN",
  DE: "EUR",
  FR: "EUR",
  IT: "EUR",
  ES: "EUR",
  NL: "EUR",
  BE: "EUR",
  AT: "EUR",
  IE: "EUR",
  PT: "EUR",
  FI: "EUR",
  GR: "EUR",
  LU: "EUR",
  GB: "GBP",
  CH: "CHF",
  SE: "SEK",
  NO: "NOK",
  DK: "DKK",
  PL: "PLN",
  CZ: "CZK",
  HU: "HUF",
  RO: "RON",
  TR: "TRY",
  AE: "AED",
  SA: "SAR",
  QA: "QAR",
  KW: "KWD",
  BH: "BHD",
  OM: "OMR",
  JO: "JOD",
  IL: "ILS",
  EG: "EGP",
  MA: "MAD",
  TN: "TND",
  JP: "JPY",
  KR: "KRW",
  CN: "CNY",
  HK: "HKD",
  TW: "TWD",
  SG: "SGD",
  MY: "MYR",
  TH: "THB",
  ID: "IDR",
  PH: "PHP",
  VN: "VND",
  IN: "INR",
  PK: "PKR",
  AU: "AUD",
  NZ: "NZD",
  BR: "BRL",
  AR: "ARS",
  CL: "CLP",
  CO: "COP",
  ZA: "ZAR",
  NG: "NGN"
};
var currencyForCountry = (c) => COUNTRY_CURRENCY[(c || "US").toUpperCase()] || "USD";
function findChrome() {
  const paths = [
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    `${process.env.LOCALAPPDATA || ""}\\Google\\Chrome\\Application\\chrome.exe`,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium-browser"
  ];
  return paths.find((p) => {
    try {
      return p && fs.existsSync(p);
    } catch {
      return false;
    }
  });
}
async function connectCDP(port) {
  for (let i = 0; i < 40; i++) {
    try {
      const targets = await new Promise((res, rej) => {
        http.get(`http://127.0.0.1:${port}/json`, (r) => {
          let d = "";
          r.on("data", (c) => d += c);
          r.on("end", () => {
            try {
              res(JSON.parse(d));
            } catch (e) {
              rej(e);
            }
          });
        }).on("error", rej);
      });
      const page = targets.find((t) => t.type === "page");
      if (page && page.webSocketDebuggerUrl) {
        const ws = new WebSocket(page.webSocketDebuggerUrl, {
          perMessageDeflate: false,
          maxPayload: 256 * 1024 * 1024
        });
        await new Promise((r, rej) => {
          ws.on("open", r);
          ws.on("error", rej);
        });
        return ws;
      }
    } catch {
    }
    await sleep(400);
  }
  return null;
}
function cdpClient(ws) {
  let id = 0;
  const pending = /* @__PURE__ */ new Map();
  const listeners = /* @__PURE__ */ new Map();
  let alive = true;
  ws.on("message", (msg) => {
    let m;
    try {
      m = JSON.parse(msg.toString());
    } catch {
      return;
    }
    if (m.id && pending.has(m.id)) {
      const p = pending.get(m.id);
      pending.delete(m.id);
      clearTimeout(p.timer);
      m.error ? p.reject(new Error(m.error.message)) : p.resolve(m.result);
    } else if (m.method && listeners.has(m.method)) {
      listeners.get(m.method).forEach((fn) => {
        try {
          fn(m.params, m.sessionId || null);
        } catch {
        }
      });
    }
  });
  const rejectPending = (reason) => {
    alive = false;
    for (const p of pending.values()) {
      clearTimeout(p.timer);
      p.reject(new Error(reason));
    }
    pending.clear();
  };
  ws.on("close", () => rejectPending("CDP connection closed"));
  ws.on("error", (e) => rejectPending(`CDP connection error: ${e.message}`));
  return {
    send(method, params = {}, sessionId = null, timeoutMs = 3e4) {
      if (!alive) return Promise.reject(new Error("CDP closed"));
      const i = ++id;
      return new Promise((res, rej) => {
        const timer = setTimeout(() => {
          if (pending.has(i)) {
            pending.delete(i);
            rej(new Error(`CDP timeout after ${timeoutMs}ms: ${method}`));
          }
        }, timeoutMs);
        pending.set(i, { resolve: res, reject: rej, timer });
        const message = { id: i, method, params };
        if (sessionId) message.sessionId = sessionId;
        try {
          ws.send(JSON.stringify(message));
        } catch (e) {
          clearTimeout(timer);
          pending.delete(i);
          rej(e);
        }
      });
    },
    on(method, fn) {
      if (!listeners.has(method)) listeners.set(method, []);
      listeners.get(method).push(fn);
      return () => {
        const handlers = listeners.get(method);
        if (!handlers) return;
        const index = handlers.indexOf(fn);
        if (index !== -1) handlers.splice(index, 1);
        if (!handlers.length) listeners.delete(method);
      };
    },
    isAlive: () => alive
  };
}
function startProxyRelay(proxyStr, localPort) {
  return new Promise((resolve, reject) => {
    let pUrl;
    try {
      pUrl = new URL(proxyStr.startsWith("http") ? proxyStr : `http://${proxyStr}`);
    } catch {
      return reject(new Error("bad proxy"));
    }
    const authH = "Basic " + Buffer.from(`${decodeURIComponent(pUrl.username || "")}:${decodeURIComponent(pUrl.password || "")}`).toString("base64");
    const host = pUrl.hostname;
    const port = parseInt(pUrl.port) || 823;
    const srv = http.createServer((req, res) => {
      const p = http.request({
        hostname: host,
        port,
        path: req.url,
        method: req.method,
        headers: { ...req.headers, "Proxy-Authorization": authH }
      }, (pR) => {
        res.writeHead(pR.statusCode, pR.headers);
        pR.pipe(res);
      });
      req.pipe(p);
      p.on("error", () => {
        try {
          res.end();
        } catch {
        }
      });
    });
    srv.on("connect", (req, sock, head) => {
      const t = http.request({
        hostname: host,
        port,
        method: "CONNECT",
        path: req.url,
        headers: { "Proxy-Authorization": authH, Host: req.url }
      });
      t.on("connect", (_, pS) => {
        sock.write("HTTP/1.1 200 Connection Established\r\n\r\n");
        if (head && head.length) pS.write(head);
        pS.pipe(sock);
        sock.pipe(pS);
        pS.on("error", () => {
          try {
            sock.end();
          } catch {
          }
        });
        sock.on("error", () => {
          try {
            pS.end();
          } catch {
          }
        });
      });
      t.on("error", () => {
        try {
          sock.end();
        } catch {
        }
      });
      t.end();
    });
    srv.listen(localPort, "127.0.0.1", () => resolve({
      close: () => {
        try {
          srv.close();
        } catch {
        }
      }
    }));
  });
}
var sharedRL = null;
var activeQuestionAbort = null;
var activePrompt = null;
function getRL() {
  if (!sharedRL) sharedRL = readline.createInterface({ input: process.stdin, output: process.stdout });
  return sharedRL;
}
function writeLine(text) {
  if (activePrompt && sharedRL) {
    process.stdout.write("\r\x1B[2K" + text + "\n");
    process.stdout.write(activePrompt + (sharedRL.line || ""));
    return;
  }
  process.stdout.write(text + "\n");
}
function ask(q) {
  activePrompt = q.split("\n").pop();
  return new Promise((resolve) => {
    const controller = new AbortController();
    let settled = false;
    const finish = (value) => {
      if (settled) return;
      settled = true;
      activePrompt = null;
      if (activeQuestionAbort === cancel) activeQuestionAbort = null;
      resolve(value);
    };
    const cancel = () => {
      controller.abort();
      finish(null);
    };
    activeQuestionAbort = cancel;
    getRL().question(q, { signal: controller.signal }, (answer) => finish(answer.trim()));
  });
}
function cancelActiveQuestion() {
  if (activeQuestionAbort) activeQuestionAbort();
}
var preferredExitProvider = null;
async function probeExitCountry(cdp, sessionId = null, perProviderTimeoutMs = 4e3) {
  const startedAt2 = Date.now();
  try {
    const res = await cdp.send("Runtime.evaluate", {
      expression: `(async () => {
        // ipapi.co refuses requests from an opaque (null) origin, which is what
        // an about:blank probe page has, so it is not used here.
        const providers = [
          {url:'https://api.country.is/', field:'country', name:'api.country.is'},
          {url:'https://ipwho.is/', field:'country_code', name:'ipwho.is'},
          {url:'https://ipinfo.io/json', field:'country', name:'ipinfo.io'}
        ];
        const preferred = ${JSON.stringify(preferredExitProvider)};
        if (preferred) {
          const index = providers.findIndex(p => p.name === preferred);
          if (index > 0) providers.unshift(providers.splice(index, 1)[0]);
        }
        for (const provider of providers) {
          const controller = new AbortController();
          const timer = setTimeout(() => controller.abort(), ${perProviderTimeoutMs});
          try {
            const joiner = provider.url.includes('?') ? '&' : '?';
            const response = await fetch(provider.url + joiner + '_=' + Date.now(), {
              cache: 'no-store',
              signal: controller.signal
            });
            const data = await response.json();
            const country = String(data[provider.field] || '').toUpperCase();
            if (response.ok && /^[A-Z]{2}$/.test(country)) {
              return JSON.stringify({ok:true,country,provider:provider.name});
            }
          } catch {}
          finally { clearTimeout(timer); }
        }
        return JSON.stringify({ok:false});
      })()`,
      awaitPromise: true,
      returnByValue: true,
      timeout: perProviderTimeoutMs * 4 + 6e3
    }, sessionId, perProviderTimeoutMs * 4 + 16e3);
    const route = JSON.parse(res.result?.value || "{}");
    route.elapsedMs = Date.now() - startedAt2;
    if (route.ok && route.provider) preferredExitProvider = route.provider;
    return route;
  } catch (e) {
    return { ok: false, error: e.message, elapsedMs: Date.now() - startedAt2 };
  }
}
var OFFER_DETECTION_EXPRESSION = `(() => {
  try {
    if (!document || !document.body) return JSON.stringify({found:false});

    const visible = el => {
      if (!el || el.offsetParent === null) return false;
      const rect = el.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    };
    const text = el => (el.textContent || '').replace(/\\s+/g, ' ').trim();

    // Priority 1: an actual free-trial offer, which we want to skip.
    const claimBtn = document.querySelector('button[aria-label="Claim offer"]');
    if (visible(claimBtn)) return JSON.stringify({found:true, type:'trial', mode:'claim'});
    const trialLabels = ['free offer', 'claim free offer', 'start free trial', 'try it free'];
    const freeBtn = Array.from(document.querySelectorAll('button, a[role="button"]'))
      .find(el => visible(el) && trialLabels.includes(text(el).toLowerCase()));
    if (freeBtn) return JSON.stringify({found:true, type:'trial', mode:'free', buttonLabel: text(freeBtn)});

    // Priority 2: any upgrade control. Its presence without a trial offer means
    // no free trial is available, which is exactly when the pay link is wanted.
    // The plain "Upgrade" entry points are rendered as a <button> in the top bar
    // and as a clickable <span> beside the account in the sidebar.
    const upgradeLabels = [
      'upgrade', 'upgrade plan', 'upgrade to plus', 'upgrade to chatgpt plus',
      'get plus', 'rejoin plus', 'subscribe to plus', 'resubscribe to plus',
      'reactivate plus', 'go plus'
    ];
    const upgradeEl = Array.from(document.querySelectorAll('button, a, span'))
      .find(el => visible(el) &&
        el.children.length <= 1 && // allows a single icon child, rejects wrappers
        upgradeLabels.includes(text(el).toLowerCase()));
    if (!upgradeEl) return JSON.stringify({found:false});

    const urlSuggestsPricing = location.hash.includes('pricing') ||
      location.pathname.includes('pricing') ||
      location.search.includes('promo_campaign');
    const inDialog = !!upgradeEl.closest('[role="dialog"], dialog');
    const bodyText = text(document.body).toLowerCase();
    const mentionsPlusPlan = bodyText.includes('chatgpt plus') || bodyText.includes('your ai assistant');
    const showsMonthlyPrice = /\\/\\s*month|per month|\\/mo\\b/i.test(bodyText);

    const surface = urlSuggestsPricing ? 'pricing-url'
      : inDialog ? 'plan-dialog'
      : (mentionsPlusPlan && showsMonthlyPrice) ? 'plan-cards'
      : 'upgrade-entry';

    return JSON.stringify({
      found: true,
      type: 'noTrial',
      buttonLabel: text(upgradeEl),
      surface
    });
  } catch (e) { return JSON.stringify({found:false, err:e.message}); }
})()`;
async function detectOffer(cdp) {
  try {
    const res = await cdp.send("Runtime.evaluate", {
      expression: OFFER_DETECTION_EXPRESSION,
      returnByValue: true,
      timeout: 5e3
    });
    return JSON.parse(res.result?.value || "{}");
  } catch {
    return { found: false };
  }
}
async function openInNewTab(cdp, url) {
  await cdp.send("Target.createTarget", { url });
}
var PAY_LINK_FILE = "./pay_links.txt";
function savePayLink(result, apiSession) {
  const line = [
    (/* @__PURE__ */ new Date()).toISOString(),
    apiSession.user?.email || "unknown",
    result.country,
    result.currency,
    result.url
  ].join(" | ");
  try {
    fs.appendFileSync(PAY_LINK_FILE, line + "\n");
    return path.resolve(PAY_LINK_FILE);
  } catch {
    return null;
  }
}
var ROUTE_FAILURE_ERRORS = /(ERR_PROXY_CONNECTION_FAILED|ERR_TUNNEL_CONNECTION_FAILED|ERR_CONNECTION_RESET|ERR_CONNECTION_CLOSED|ERR_NETWORK_CHANGED|ERR_INTERNET_DISCONNECTED)/i;
function classifyRequestFailure({ errorText = "", type = "", url = null }) {
  if (!["Document", "XHR", "Fetch"].includes(type)) {
    return { relevant: false, reason: "resource type is not a document or API call" };
  }
  const routeFailure = ROUTE_FAILURE_ERRORS.test(errorText);
  const mainDocumentTimeout = type === "Document" && /ERR_TIMED_OUT/i.test(errorText);
  if (!routeFailure && !mainDocumentTimeout) {
    return { relevant: false, reason: "error is not a route failure" };
  }
  let host = "";
  try {
    host = url ? new URL(url).hostname : "";
  } catch {
  }
  if (/(^|\.)chatgpt\.com$/i.test(host)) return { relevant: true, host };
  if (!url && type === "Document") return { relevant: true, host: null };
  return { relevant: false, reason: "third-party host", host: host || null };
}
var PERSISTENT_COOKIE_EXPIRY = () => Math.floor(Date.now() / 1e3) + 30 * 24 * 60 * 60;
var MAX_COOKIE_BYTES = 4096;
function clearStaleProfileLocks(profileDir) {
  const removed = [];
  for (const name of ["SingletonLock", "SingletonCookie", "SingletonSocket"]) {
    const target = path.join(profileDir, name);
    try {
      fs.rmSync(target, { recursive: true, force: true });
      removed.push(name);
    } catch {
    }
  }
  return removed;
}
async function closeBrowserGracefully(cdp, chrome, timeoutMs = 4e3) {
  try {
    if (cdp?.isAlive?.()) {
      await cdp.send("Browser.close", {}, null, timeoutMs);
    }
  } catch {
  }
  const deadline = Date.now() + timeoutMs;
  while (chrome && chrome.exitCode === null && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  if (chrome && chrome.exitCode === null) {
    try {
      chrome.kill();
    } catch {
    }
  }
}
async function installSessionCookieFamily(cdp, cookies) {
  const installed = [];
  for (const cookie of Array.isArray(cookies) ? cookies : []) {
    const name = cookie?.name;
    const value = cookie?.value;
    if (!name || typeof value !== "string" || !value.length) continue;
    if (name === COOKIE_NAME) continue;
    const domain = cookie.domain || ".chatgpt.com";
    try {
      const result = await cdp.send("Network.setCookie", {
        name,
        value,
        domain,
        path: cookie.path || "/",
        secure: cookie.secure !== false,
        httpOnly: !!cookie.httpOnly,
        sameSite: cookie.sameSite || "Lax",
        // Without an expiry Chrome treats it as a session cookie and never
        // writes it to disk, so a persistent profile would lose it on exit.
        expires: typeof cookie.expires === "number" ? cookie.expires : PERSISTENT_COOKIE_EXPIRY()
      });
      if (result?.success !== false) installed.push(`${name}@${domain}`);
    } catch {
    }
  }
  return installed;
}
async function installSessionCookie(cdp, sessionToken) {
  if (sessionToken.length + COOKIE_NAME.length + 1 > MAX_COOKIE_BYTES) return false;
  try {
    const result = await cdp.send("Network.setCookie", {
      name: COOKIE_NAME,
      value: sessionToken,
      url: "https://chatgpt.com/",
      path: "/",
      secure: true,
      httpOnly: true,
      sameSite: "Lax",
      expires: PERSISTENT_COOKIE_EXPIRY()
    });
    return result?.success !== false;
  } catch {
    return false;
  }
}
var EDGE_AFFINITY_COOKIES = [
  "__oailb",
  "__cflb",
  "oai-hlib",
  "oai-sc",
  "oai-allow-ne",
  "oai-nav-state"
];
var CHALLENGE_COOKIES = ["__cf_bm", "_cfuvid", "cf_clearance"];
async function clearRegionPinnedCookies(cdp, includeChallenge = false) {
  const names = includeChallenge ? [...EDGE_AFFINITY_COOKIES, ...CHALLENGE_COOKIES] : EDGE_AFFINITY_COOKIES;
  const cleared = [];
  for (const name of names) {
    for (const domain of ["chatgpt.com", ".chatgpt.com"]) {
      try {
        await cdp.send("Network.deleteCookies", { name, domain, path: "/" });
        cleared.push(`${name}@${domain}`);
      } catch {
      }
    }
  }
  return cleared;
}
async function readSessionCookies(cdp) {
  try {
    const result = await cdp.send("Network.getCookies", {
      urls: ["https://chatgpt.com/", "https://chatgpt.com/api/auth/session"]
    });
    const cookies = result.cookies || [];
    const whole = cookies.filter((c) => c.name === COOKIE_NAME && (c.value || "").length >= 20);
    if (whole.length) return whole;
    const chunkPattern = new RegExp(`^${COOKIE_NAME.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\.(\\d+)$`);
    const chunks = cookies.map((c) => ({ cookie: c, index: Number((c.name.match(chunkPattern) || [])[1]) })).filter((entry) => Number.isInteger(entry.index)).sort((a, b) => a.index - b.index);
    if (!chunks.length) return [];
    const value = chunks.map((entry) => entry.cookie.value || "").join("");
    if (value.length < 20) return [];
    return [{ ...chunks[0].cookie, name: COOKIE_NAME, value, chunked: true }];
  } catch {
    return [];
  }
}
function parseSessionTokenFromSetCookie(headerValue) {
  const lines = String(headerValue || "").split("\n").map((line) => line.trim());
  const readValue = (line, prefix) => line.slice(prefix.length).split(";")[0].trim();
  const whole = lines.find((line) => line.startsWith(`${COOKIE_NAME}=`));
  if (whole) {
    const value2 = readValue(whole, `${COOKIE_NAME}=`);
    if (value2.length >= 20) return value2;
  }
  const chunkPattern = new RegExp(`^${COOKIE_NAME.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\.(\\d+)=`);
  const chunks = lines.map((line) => ({ line, index: Number((line.match(chunkPattern) || [])[1]) })).filter((entry) => Number.isInteger(entry.index)).sort((a, b) => a.index - b.index);
  if (!chunks.length) return null;
  const value = chunks.map((entry) => readValue(entry.line, entry.line.slice(0, entry.line.indexOf("=") + 1))).join("");
  return value.length >= 20 ? value : null;
}
async function main() {
  console.log("\n" + "=".repeat(60));
  console.log("  \u{1F3AF} ChatGPT Smart (v9.3 - Persistent Login Profile)");
  console.log("=".repeat(60) + "\n");
  const diagnosticsPath = path.join(os.tmpdir(), "chatgpt_proxy_diagnostics.jsonl");
  try {
    fs.writeFileSync(diagnosticsPath, "");
  } catch {
  }
  const trace = (event, details = {}, line = null) => {
    try {
      fs.appendFileSync(diagnosticsPath, JSON.stringify({
        at: (/* @__PURE__ */ new Date()).toISOString(),
        sinceStartMs: Date.now() - startedAt,
        event,
        ...details
      }) + "\n");
    } catch {
    }
    if (TRACE_ENABLED && line) writeLine(`  ${elapsedStamp()} ${line}`);
  };
  console.log(`  \u{1F9FE} Trace stream: ${TRACE_ENABLED ? "ON" : "OFF"} (TRACE=0 silence | TRACE_ASSETS=1 assets | TRACE_TELEMETRY=1 analytics)`);
  console.log(`  \u{1F5C2}\uFE0F  Diagnostics file: ${diagnosticsPath}
`);
  if (!fs.existsSync("./session_api.json")) {
    console.error("\u274C session_api.json not found");
    process.exit(1);
  }
  const apiSession = JSON.parse(fs.readFileSync("./session_api.json", "utf8"));
  let sessionToken = apiSession.sessionToken;
  if (typeof sessionToken !== "string") {
    console.error("\u274C sessionToken missing");
    process.exit(1);
  }
  sessionToken = sessionToken.trim().replace(/[\r\n\t]/g, "");
  if (sessionToken.length < 20) {
    console.error("\u274C sessionToken invalid");
    process.exit(1);
  }
  const idp = apiSession.user?.idp || "unknown";
  const cookieSize = sessionToken.length + COOKIE_NAME.length + 1;
  console.log(`  \u{1F4E7} ${apiSession.user?.email || "unknown"}`);
  console.log(`  \u{1F510} IDP: ${idp}${idp === "google-oauth2" ? " (Google OAuth)" : ""}`);
  console.log(`  \u{1F4CF} Cookie size: ${cookieSize} bytes ${cookieSize > 4096 ? "(Fetch injection active)" : ""}`);
  console.log(`  \u{1F3AB} accessToken in file: ${apiSession.accessToken ? "YES (fallback ready)" : "NO"}`);
  console.log("");
  let proxies = [];
  try {
    const pf = JSON.parse(fs.readFileSync("./proxies.json", "utf8"));
    proxies = (Array.isArray(pf) ? pf : []).map((p) => typeof p === "string" ? p : p.proxyString || p.proxy_string || p.url).filter(Boolean);
  } catch {
  }
  let proxyStr = null;
  if (proxies.length > 0) {
    const useProxy = (await ask("  Use proxy? [Y/n]: ") || "").toLowerCase();
    if (useProxy !== "n") {
      proxyStr = proxies[0];
      if (proxyStr.includes("{COUNTRY}")) proxyStr = proxyStr.replace(/\{COUNTRY\}/gi, "us");
      console.log(`  \u2713 Using proxy: ${proxyStr.slice(0, 50)}...`);
    } else {
      console.log("  \u2713 Direct connection");
    }
  } else {
    console.log("  \u2139\uFE0F No proxies.json - direct connection");
  }
  const localPort = 12345 + Math.floor(Math.random() * 100);
  let proxyHandle = null;
  if (proxyStr) proxyHandle = await startProxyRelay(proxyStr, localPort);
  const chromePath = findChrome();
  if (!chromePath) {
    console.error("\u274C Chrome not found");
    process.exit(1);
  }
  const userDir = USE_TEMP_PROFILE ? path.join(os.tmpdir(), `real_chrome_${Date.now()}`) : PROFILE_DIR;
  const profileIsNew = !fs.existsSync(path.join(userDir, "Default"));
  fs.mkdirSync(userDir, { recursive: true });
  fs.mkdirSync(path.join(userDir, "Default"), { recursive: true });
  console.log(`  \u{1F4C1} Profile: ${USE_TEMP_PROFILE ? "temporary (discarded on exit)" : userDir}`);
  if (!USE_TEMP_PROFILE && profileIsNew) {
    console.log("     First run for this profile. Use --login once to sign in normally,");
    console.log("     so the complete cookie family is stored and survives IP changes.");
  }
  if (profileIsNew) fs.writeFileSync(path.join(userDir, "Default", "Preferences"), JSON.stringify({
    profile: {
      exit_type: "Normal",
      exited_cleanly: true,
      last_engagement_time: Date.now() * 1e3,
      default_content_setting_values: { notifications: 2 }
    },
    intl: { accept_languages: "en-US,en" },
    browser: { has_seen_welcome_page: true },
    signin: { allowed: true },
    credentials_enable_service: false
  }));
  const cdpPort = 9333 + Math.floor(Math.random() * 100);
  const extensionsDir = path.resolve("./extensions");
  const extensionPaths = [];
  if (fs.existsSync(extensionsDir)) {
    try {
      const entries = fs.readdirSync(extensionsDir, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isDirectory()) continue;
        const extDir = path.join(extensionsDir, entry.name);
        let manifestDir = null;
        if (fs.existsSync(path.join(extDir, "manifest.json"))) {
          manifestDir = extDir;
          console.log(`  \u{1F9E9} Found extension: ${entry.name}`);
        } else {
          try {
            const subs = fs.readdirSync(extDir, { withFileTypes: true }).filter((s) => s.isDirectory());
            for (const sub of subs) {
              const subDir = path.join(extDir, sub.name);
              if (fs.existsSync(path.join(subDir, "manifest.json"))) {
                manifestDir = subDir;
                console.log(`  \u{1F9E9} Found extension: ${entry.name}/${sub.name}`);
                break;
              }
            }
          } catch {
          }
        }
        if (!manifestDir) continue;
        const tempExtDir = path.join(userDir, "ext_" + entry.name);
        try {
          const copyRecursive = (src, dest) => {
            fs.mkdirSync(dest, { recursive: true });
            for (const it of fs.readdirSync(src, { withFileTypes: true })) {
              const s = path.join(src, it.name);
              const d = path.join(dest, it.name);
              if (it.isDirectory()) copyRecursive(s, d);
              else fs.copyFileSync(s, d);
            }
          };
          copyRecursive(manifestDir, tempExtDir);
          const manifestPath = path.join(tempExtDir, "manifest.json");
          const manifestRaw = fs.readFileSync(manifestPath, "utf8").replace(/^\uFEFF/, "");
          const manifest = JSON.parse(manifestRaw);
          delete manifest.key;
          delete manifest.update_url;
          delete manifest.differential_fingerprint;
          fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
          extensionPaths.push(tempExtDir);
          console.log(`     \u2514\u2500 Prepared: "${manifest.name || "unknown"}" (mv${manifest.manifest_version || "?"})`);
        } catch (e) {
          console.log(`     \u26A0\uFE0F Failed to prepare ${entry.name}: ${e.message}`);
        }
      }
    } catch (e) {
      console.log(`  \u26A0\uFE0F Extensions folder read error: ${e.message}`);
    }
  }
  const args = [
    `--remote-debugging-port=${cdpPort}`,
    `--user-data-dir=${userDir}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-blink-features=AutomationControlled",
    "--exclude-switches=enable-automation",
    "--start-maximized",
    "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "--enable-features=NetworkService,NetworkServiceInProcess",
    "--disable-features=OptimizationHints,MediaRouter"
  ];
  if (extensionPaths.length > 0) {
    args.push(`--load-extension=${extensionPaths.join(",")}`);
    console.log(`  \u2705 Loading ${extensionPaths.length} extension(s)`);
  } else if (fs.existsSync(extensionsDir)) {
    console.log(`  \u2139\uFE0F ./extensions/ exists but no valid extensions found`);
  }
  if (proxyStr) {
    args.push(`--proxy-server=http://127.0.0.1:${localPort}`);
    args.push("--proxy-bypass-list=<-loopback>");
  }
  args.push("about:blank");
  console.log("\n  \u{1F680} Opening Chrome...");
  let chrome = spawn(chromePath, args, { stdio: "ignore" });
  await sleep(800);
  let cleaning = false;
  let gracefulCdp = null;
  const cleanup = () => {
    if (cleaning) return;
    cleaning = true;
    void closeBrowserGracefully(gracefulCdp, chrome);
    if (proxyHandle) proxyHandle.close();
    if (sharedRL) {
      try {
        sharedRL.close();
      } catch {
      }
    }
    if (USE_TEMP_PROFILE) {
      setTimeout(() => {
        try {
          fs.rmSync(userDir, { recursive: true, force: true });
        } catch {
        }
      }, 1e3);
    }
  };
  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, async () => {
      console.log("\n  \u{1F44B} Shutting down...");
      cleaning = true;
      await closeBrowserGracefully(gracefulCdp, chrome, 3e3);
      if (proxyHandle) proxyHandle.close();
      if (sharedRL) {
        try {
          sharedRL.close();
        } catch {
        }
      }
      if (USE_TEMP_PROFILE) {
        try {
          fs.rmSync(userDir, { recursive: true, force: true });
        } catch {
        }
      }
      process.exit(0);
    });
  }
  let ws = await connectCDP(cdpPort);
  if (!ws && !USE_TEMP_PROFILE) {
    const removed = clearStaleProfileLocks(userDir);
    console.log(`  \u267B\uFE0F  Debugging port unavailable; cleared stale profile locks (${removed.join(", ") || "none"}) and retrying once...`);
    try {
      chrome.kill();
    } catch {
    }
    await sleep(1200);
    chrome = spawn(chromePath, args, { stdio: "ignore" });
    await sleep(1200);
    ws = await connectCDP(cdpPort);
  }
  if (!ws) {
    console.error("\u274C Could not attach to Chrome.");
    if (!USE_TEMP_PROFILE) {
      console.error(`   Close any Chrome window still using ${userDir}, then run again.`);
      console.error("   Or start with --temp-profile to ignore the saved profile.");
    }
    cleanup();
    process.exit(1);
  }
  const cdp = cdpClient(ws);
  gracefulCdp = cdp;
  try {
    await cdp.send("Network.enable");
    await cdp.send("Page.enable");
    await cdp.send("Runtime.enable");
  } catch (e) {
    console.error("\u274C CDP setup:", e.message);
    cleanup();
    process.exit(1);
  }
  try {
    await cdp.send("Page.setBypassCSP", { enabled: true });
  } catch {
  }
  try {
    await cdp.send("Network.setBypassServiceWorker", { bypass: true });
  } catch {
  }
  let serverSessionToken = null;
  let pageIsChatGPT = false;
  let anonymousRenderSeenAt = 0;
  let anonymousRepairPromise = null;
  let anonymousRepairAttempts = 0;
  let anonymousNoticeShown = false;
  let anonymousOfferPending = false;
  let lastAnonymousRepairAt = 0;
  let monitoringReady = false;
  let lastChatGPTActivityAt = Date.now();
  const noteChatGPTActivity = (url) => {
    if (/(^|\/\/|\.)chatgpt\.com(\/|$)/i.test(String(url || ""))) lastChatGPTActivityAt = Date.now();
  };
  function adoptSessionToken(token, source) {
    if (!token || token === sessionToken) return false;
    const previousFingerprint = fingerprint(sessionToken);
    sessionToken = token;
    trace("session_cookie_rotated", {
      source,
      previousFingerprint,
      fingerprint: fingerprint(sessionToken),
      cookieSize: sessionToken.length + COOKIE_NAME.length + 1
    }, `\u{1F511} session cookie updated (${source}): ${previousFingerprint} \u2192 ${fingerprint(sessionToken)}`);
    return true;
  }
  async function syncActiveSessionCookie(source) {
    const cookies = await readSessionCookies(cdp);
    if (!cookies.length) return false;
    const distinct = new Set(cookies.map((c) => c.value));
    if (distinct.size > 1) {
      const keep = serverSessionToken && distinct.has(serverSessionToken) ? serverSessionToken : cookies[0].value;
      for (const cookie of cookies) {
        if (cookie.value === keep) continue;
        try {
          await cdp.send("Network.deleteCookies", {
            name: COOKIE_NAME,
            domain: cookie.domain,
            path: cookie.path || "/"
          });
        } catch {
        }
      }
      trace("session_cookie_duplicates_resolved", {
        source,
        removed: distinct.size - 1,
        domains: cookies.map((c) => c.domain)
      }, `\u{1F511} removed ${distinct.size - 1} duplicate session cookie(s)`);
      adoptSessionToken(keep, `${source} (deduplicated)`);
      return true;
    }
    if (serverSessionToken) return false;
    return adoptSessionToken(cookies[0].value, source);
  }
  try {
    await cdp.send("Log.enable");
  } catch {
  }
  const inFlight = /* @__PURE__ */ new Map();
  const isAsset = (type) => ASSET_TYPES.has(type);
  cdp.on("Network.requestWillBeSent", (params) => {
    const url = params.request?.url || "";
    const type = params.type || "Other";
    noteChatGPTActivity(url);
    if (/chatgpt\.com\/backend-anon\//i.test(url)) {
      anonymousRenderSeenAt = Date.now();
      if (monitoringReady) void repairAnonymousRender();
    }
    inFlight.set(params.requestId, {
      url,
      type,
      method: params.request?.method || "GET",
      startedAt: Date.now()
    });
    const quiet = isAsset(type) && !TRACE_ASSETS || isTelemetryUrl(url);
    trace("request", {
      requestId: params.requestId,
      method: params.request?.method,
      type,
      url,
      headers: redactHeaders(params.request?.headers)
    }, quiet ? null : `\u2192 ${params.request?.method || "GET"} ${type} ${shortenUrl(url)}`);
  });
  cdp.on("Network.requestWillBeSentExtraInfo", (params) => {
    const blocked = (params.associatedCookies || []).filter((entry) => (entry.blockedReasons || []).length).filter((entry) => entry.cookie?.name === COOKIE_NAME || (entry.blockedReasons || []).some((reason) => reason !== "DomainMismatch")).map((entry) => `${entry.cookie?.name}:${(entry.blockedReasons || []).join("|")}`);
    if (!blocked.length) return;
    trace("cookies_blocked_on_request", {
      requestId: params.requestId,
      blocked
    }, `\u{1F6AB} cookies blocked \u2192 ${blocked.join(", ")}`);
  });
  cdp.on("Network.responseReceived", (params) => {
    const info = inFlight.get(params.requestId);
    const type = params.type || info?.type || "Other";
    const response = params.response || {};
    noteChatGPTActivity(response.url);
    const durationMs = info ? Date.now() - info.startedAt : null;
    const quiet = response.status < 400 && (isAsset(type) && !TRACE_ASSETS || isTelemetryUrl(response.url));
    trace("response", {
      requestId: params.requestId,
      status: response.status,
      type,
      url: safeUrl(response.url),
      remoteIP: response.remoteIPAddress || null,
      protocol: response.protocol || null,
      fromDiskCache: !!response.fromDiskCache,
      fromServiceWorker: !!response.fromServiceWorker,
      durationMs,
      headers: redactHeaders(response.headers)
    }, quiet ? null : `\u2190 ${response.status} ${type} ${shortenUrl(response.url)} ip=${response.remoteIPAddress || "?"}${durationMs === null ? "" : ` ${durationMs}ms`}`);
  });
  cdp.on("Network.loadingFinished", (params) => {
    inFlight.delete(params.requestId);
  });
  cdp.on("Network.responseReceived", (params) => {
    const response = params.response || {};
    if (response.status !== 403 && response.status !== 503) return;
    if (!/chatgpt\.com/i.test(response.url || "")) return;
    const headers = Object.fromEntries(
      Object.entries(response.headers || {}).map(([k, v]) => [k.toLowerCase(), String(v)])
    );
    const mitigated = headers["cf-mitigated"] || null;
    const isHtml = /text\/html/i.test(headers["content-type"] || "");
    if (!mitigated && !isHtml) return;
    challengeSeenAt = Date.now();
    trace("cloudflare_challenge", {
      status: response.status,
      url: response.url,
      mitigated,
      remoteIP: response.remoteIPAddress || null
    }, `\u{1F6E1}\uFE0F  Cloudflare challenge ${response.status}${mitigated ? ` (${mitigated})` : ""} on ${shortenUrl(response.url)}`);
  });
  cdp.on("Network.responseReceivedExtraInfo", (params) => {
    const rawSetCookie = params.headers?.["set-cookie"] || params.headers?.["Set-Cookie"] || "";
    const setCookieNames = String(rawSetCookie).split("\n").map((line) => line.split("=")[0].trim()).filter(Boolean);
    const blocked = (params.blockedCookies || []).map((entry) => `${entry.cookie?.name || entry.cookieLine?.split("=")[0]}:${(entry.blockedReasons || []).join("|")}`);
    if (setCookieNames.length || blocked.length) {
      trace("response_cookies", {
        requestId: params.requestId,
        setCookieNames,
        blocked
      }, `\u{1F36A} set-cookie [${setCookieNames.join(", ") || "none"}]${blocked.length ? ` blocked [${blocked.join(", ")}]` : ""}`);
    }
    const issued = parseSessionTokenFromSetCookie(rawSetCookie);
    if (issued) {
      serverSessionToken = issued;
      adoptSessionToken(issued, "server set-cookie");
    }
  });
  cdp.on("Page.frameNavigated", (params) => {
    if (params.frame?.parentId) return;
    let host = "";
    try {
      host = new URL(params.frame?.url || "").hostname;
    } catch {
    }
    pageIsChatGPT = host === "chatgpt.com" && !params.frame?.unreachableUrl;
    trace("frame_navigated", {
      url: safeUrl(params.frame?.url),
      host,
      status: params.frame?.unreachableUrl ? "unreachable" : "ok"
    }, `\u{1F9ED} navigated ${shortenUrl(params.frame?.url || "")}`);
  });
  cdp.on("Page.domContentEventFired", () => {
    trace("dom_content_loaded", {}, "\u{1F4C4} DOMContentLoaded");
  });
  cdp.on("Page.loadEventFired", () => {
    trace("load_event", {}, "\u{1F4C4} load event fired");
  });
  cdp.on("Runtime.exceptionThrown", (params) => {
    const detail = params.exceptionDetails || {};
    trace("page_exception", {
      text: detail.text,
      message: detail.exception?.description?.slice(0, 300) || null,
      url: detail.url || null
    }, `\u{1F4A5} page exception: ${(detail.exception?.description || detail.text || "").split("\n")[0].slice(0, 160)}`);
  });
  cdp.on("Runtime.consoleAPICalled", (params) => {
    if (!["error", "warning", "assert"].includes(params.type)) return;
    const text = (params.args || []).map((arg) => arg.value ?? arg.description ?? arg.type).join(" ").slice(0, 220);
    trace("page_console", { level: params.type, text }, `\u{1F5A5}\uFE0F  console.${params.type}: ${text}`);
  });
  cdp.on("Log.entryAdded", (params) => {
    const entry = params.entry || {};
    if (!["error", "warning"].includes(entry.level)) return;
    trace("browser_log", {
      level: entry.level,
      source: entry.source,
      text: entry.text,
      url: entry.url || null
    }, `\u{1F4D5} ${entry.source}/${entry.level}: ${String(entry.text).slice(0, 200)}`);
  });
  try {
    await cdp.send("Page.addScriptToEvaluateOnNewDocument", {
      source: `
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
        window.chrome = { runtime: {} };
      `
    });
  } catch {
  }
  console.log("  \u{1F527} Enabling Fetch interception...");
  await cdp.send("Fetch.enable", {
    patterns: [
      { urlPattern: "https://chatgpt.com/*", requestStage: "Request" },
      { urlPattern: "https://*.chatgpt.com/*", requestStage: "Request" },
      { urlPattern: "https://*.openai.com/*", requestStage: "Request" }
    ]
  });
  const pausedRequests = /* @__PURE__ */ new Map();
  let requestCounter = 0;
  let injectedCounter = 0;
  let successCounter = 0;
  let timeoutCounter = 0;
  let errorCounter = 0;
  const shortUrl = (url) => shortenUrl(url, 60);
  cdp.on("Fetch.requestPaused", async (params) => {
    const requestId = params.requestId;
    const url = params.request?.url || "";
    const shouldInject = /^https:\/\/([a-z0-9-]+\.)?chatgpt\.com\//i.test(url);
    const short = shortUrl(url);
    const resourceType = params.resourceType || "Other";
    requestCounter++;
    const cookieHeader = Object.entries(params.request?.headers || {}).find(([name]) => name.toLowerCase() === "cookie");
    const browserSentSession = !!cookieHeader && new RegExp(`(?:^|;\\s*)${COOKIE_NAME.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}=[^;]{20,}`).test(String(cookieHeader[1]));
    const quietIntercept = isAsset(resourceType) && !TRACE_ASSETS || isTelemetryUrl(url);
    {
      trace("fetch_intercepted", {
        requestId,
        method: params.request?.method,
        resourceType,
        url,
        inject: shouldInject && !browserSentSession,
        browserSentSession,
        cookieFingerprint: shouldInject && !browserSentSession ? fingerprint(sessionToken) : null
      }, quietIntercept ? null : `\u21E2 intercept ${params.request?.method || "GET"} ${resourceType} ${short}${!shouldInject ? " inject=no" : browserSentSession ? " cookie=browser" : ` inject=${fingerprint(sessionToken)}`}`);
    }
    const rescueTimer = setTimeout(async () => {
      timeoutCounter++;
      trace(
        "fetch_rescue_timeout",
        { requestId, url, timeoutCounter },
        `\u23F1\uFE0F  rescue timeout #${timeoutCounter} ${short}`
      );
      try {
        await cdp.send("Fetch.failRequest", { requestId, errorReason: "TimedOut" });
      } catch {
      }
      pausedRequests.delete(requestId);
    }, 8e3);
    pausedRequests.set(requestId, { timer: rescueTimer, url, startedAt: Date.now() });
    try {
      if (!shouldInject || browserSentSession) {
        await cdp.send("Fetch.continueRequest", { requestId });
      } else {
        const reqHeaders = params.request.headers || {};
        const newHeaders = [];
        let hadCookie = false;
        for (const [k, v] of Object.entries(reqHeaders)) {
          if (k.toLowerCase() === "cookie") {
            hadCookie = true;
            const existing = String(v).split(/;\s*/).filter((c) => c && !c.startsWith(COOKIE_NAME + "="));
            existing.push(`${COOKIE_NAME}=${sessionToken}`);
            newHeaders.push({ name: "Cookie", value: existing.join("; ") });
          } else newHeaders.push({ name: k, value: String(v) });
        }
        if (!hadCookie) newHeaders.push({ name: "Cookie", value: `${COOKIE_NAME}=${sessionToken}` });
        await cdp.send("Fetch.continueRequest", { requestId, headers: newHeaders });
        injectedCounter++;
      }
      clearTimeout(rescueTimer);
      pausedRequests.delete(requestId);
      successCounter++;
    } catch (e) {
      clearTimeout(rescueTimer);
      pausedRequests.delete(requestId);
      const benign = /Invalid InterceptionId/i.test(e.message);
      if (!benign) errorCounter++;
      trace(
        "fetch_intercept_error",
        { requestId, url, error: e.message, benign },
        benign ? null : `\u274C intercept error #${errorCounter} ${short} \u2014 ${e.message}`
      );
      try {
        await cdp.send("Fetch.continueRequest", { requestId });
      } catch {
        try {
          await cdp.send("Fetch.failRequest", { requestId, errorReason: "Failed" });
        } catch {
        }
      }
    }
  });
  let awaitingInput = false;
  const leakDetector = setInterval(() => {
    const pending = pausedRequests.size;
    if (pending > 10) {
      const oldest = [...pausedRequests.values()].sort((a, b) => a.startedAt - b.startedAt).slice(0, 3).map((info) => `${Math.round((Date.now() - info.startedAt) / 1e3)}s:${shortUrl(info.url)}`);
      trace(
        "paused_request_backlog",
        { pending, oldest },
        `\u26A0\uFE0F  ${pending} paused requests | oldest ${oldest.join(" , ")}`
      );
    }
  }, 5e3);
  const statsReporter = setInterval(() => {
    if (requestCounter === 0) return;
    trace("request_stats", {
      total: requestCounter,
      ok: successCounter,
      timeouts: timeoutCounter,
      errors: errorCounter,
      injected: injectedCounter,
      pending: pausedRequests.size,
      inFlight: inFlight.size
    }, `\u{1F4CA} requests ${requestCounter} total | ${successCounter} ok | ${injectedCounter} cookie-injected | ${timeoutCounter} timeout | ${errorCounter} error | ${pausedRequests.size} paused`);
  }, 15e3);
  console.log("  \u2705 Interceptor ready (request rescue: 8s, verbose logging on)\n");
  const existingJarCookies = await readSessionCookies(cdp);
  if (existingJarCookies.length) {
    console.log("  \u{1F36A} Session cookie: already present in this profile (no injection needed)");
    trace("session_cookie_from_profile", { count: existingJarCookies.length });
  } else {
    const cookieInstalled = await installSessionCookie(cdp, sessionToken);
    console.log(`  \u{1F36A} Session cookie: ${cookieInstalled ? "stored in Chrome" : "header injection fallback"}`);
    trace("session_cookie_install", { success: cookieInstalled, cookieSize });
  }
  const extraCookies = await installSessionCookieFamily(cdp, apiSession.cookies);
  if (extraCookies.length) {
    console.log(`  \u{1F36A} Additional login cookies installed: ${extraCookies.length}`);
    trace("session_cookie_family_installed", { cookies: extraCookies });
  } else if (!existingJarCookies.length) {
    console.log("  \u26A0\uFE0F Only the NextAuth token is available; ChatGPT may ask to pick the account after an IP change.");
    console.log('     Run "node chatgpt_smart --login" once to store the full login state in the profile.');
  }
  console.log("  \u{1F310} Navigating to chatgpt.com...");
  await cdp.send("Page.navigate", { url: "https://chatgpt.com/" });
  await waitForChatGPTDocument(3e3, null, null, { idleGraceMs: 4e3 });
  let verifiedEmail = null;
  let initialCountry = null;
  for (let attempt = 1; attempt <= 12 && (attempt <= 5 || Date.now() - lastChatGPTActivityAt < 15e3); attempt++) {
    try {
      const res = await cdp.send("Runtime.evaluate", {
        expression: `Promise.all([
          fetch('/api/auth/session',{credentials:'include',cache:'no-store'}).then(r=>r.json()),
          fetch('/backend-api/me',{credentials:'include',cache:'no-store'}).then(r=>r.ok?r.json():({}))
        ]).then(([session,me])=>JSON.stringify({
          ok:!!(session&&session.user),
          email:session?.user?.email,
          hasToken:!!session?.accessToken,
          country:me?.country||null
        })).catch(e=>JSON.stringify({ok:false,err:e.message}))`,
        awaitPromise: true,
        returnByValue: true,
        timeout: 4e4
      }, null, 5e4);
      const parsed = JSON.parse(res.result?.value || "{}");
      if (parsed.ok && parsed.hasToken) {
        verifiedEmail = parsed.email;
        initialCountry = parsed.country || null;
        break;
      }
    } catch {
    }
    await sleep(1500);
  }
  if (verifiedEmail) console.log(`  \u2705 Logged in as: ${verifiedEmail}
`);
  else console.log("  \u26A0\uFE0F Login verification uncertain (will try anyway)\n");
  await syncActiveSessionCookie("initial login");
  if (initialCountry) console.log(`  \u{1F30D} Initial country: ${initialCountry} (${currencyForCountry(initialCountry)})
`);
  const keepAlive = setInterval(async () => {
    if (!cdp.isAlive()) {
      clearInterval(keepAlive);
      return;
    }
    try {
      await cdp.send("Browser.getVersion");
    } catch {
    }
  }, 2e4);
  const cookieSyncMonitor = setInterval(() => {
    if (cdp.isAlive() && !cleaning) void syncActiveSessionCookie("periodic sync");
  }, 5e3);
  let handled = false;
  let lastOfferSeen = false;
  let lastKnownCountry = initialCountry;
  let activeRouteCountry = null;
  let autoReloadInProgress = false;
  let checkoutInProgress = false;
  let last401At = 0;
  let lastTransportFailureAt = 0;
  let challengeSeenAt = 0;
  let lastInterestingAt = Date.now();
  let countryTick = 0;
  let snapshotTick = 0;
  let lastExitCountry = null;
  const markActivity = () => {
    lastInterestingAt = Date.now();
  };
  const inActivePeriod = () => Date.now() - lastInterestingAt < 6e4;
  let recoveryPromise = null;
  let recoveryState = "idle";
  let recoveryStateSince = Date.now();
  const pendingRecoveryReasons = /* @__PURE__ */ new Set();
  const setRecoveryState = (next) => {
    if (recoveryState === next) return;
    const heldMs = Date.now() - recoveryStateSince;
    trace(
      "recovery_state",
      { from: recoveryState, to: next, heldMs },
      `\u{1F9ED} recovery state ${recoveryState} \u2192 ${next} (${heldMs}ms)`
    );
    recoveryState = next;
    recoveryStateSince = Date.now();
  };
  async function probeChatGPTSession(sessionId = null) {
    try {
      const res = await cdp.send("Runtime.evaluate", {
        expression: `(async () => {
          const read = async url => {
            try {
              const response = await fetch(url, {credentials:'include', cache:'no-store'});
              const text = await response.text();
              let json = null;
              try { json = JSON.parse(text); } catch {}
              return {
                status: response.status,
                json,
                html: json === null && /^\\s*</.test(text),
                snippet: json === null ? text.slice(0, 80) : null
              };
            } catch (e) {
              return { status: 0, json: null, html: false, snippet: e.message };
            }
          };
          const [session, me] = await Promise.all([
            read('/api/auth/session'),
            read('/backend-api/me')
          ]);
          return JSON.stringify({
            loggedIn: !!session.json?.user,
            email: session.json?.user?.email || null,
            sessionStatus: session.status,
            status: me.status,
            country: me.json?.country || null,
            challenge: session.html || me.html,
            snippet: session.snippet || me.snippet || null
          });
        })()`,
        awaitPromise: true,
        returnByValue: true,
        timeout: 45e3
      }, sessionId, 55e3);
      return JSON.parse(res.result?.value || "{}");
    } catch (e) {
      return { status: 0, error: e.message };
    }
  }
  async function waitForHealthyProxyRoute(previousCountry, timeoutMs = 12e4, changeWindowMs = 6e3) {
    const startedProbingAt = Date.now();
    const deadline = startedProbingAt + timeoutMs;
    let probeNumber = 0;
    let lastHealthy = null;
    let targetId = null;
    try {
      const target = await cdp.send("Target.createTarget", {
        url: "about:blank",
        background: true
      });
      targetId = target.targetId;
      const attached = await cdp.send("Target.attachToTarget", {
        targetId,
        flatten: true
      });
      const probeSessionId = attached.sessionId;
      await cdp.send("Runtime.enable", {}, probeSessionId);
      try {
        await cdp.send("Page.setBypassCSP", { enabled: true }, probeSessionId);
      } catch {
      }
      while (Date.now() < deadline && cdp.isAlive() && !cleaning) {
        probeNumber++;
        setRecoveryState("probing-route");
        const providerBudget = Math.min(4e3 + (probeNumber - 1) * 3e3, 2e4);
        const route = await probeExitCountry(cdp, probeSessionId, providerBudget);
        const changed = route.ok && (!previousCountry || route.country !== previousCountry);
        trace("proxy_route_probe", {
          probeNumber,
          isolated: true,
          ok: !!route.ok,
          country: route.country || null,
          provider: route.provider || null,
          changed,
          providerBudgetMs: providerBudget,
          elapsedMs: route.elapsedMs,
          error: route.error || null
        }, `\u{1F6F0}\uFE0F  isolated route probe #${probeNumber} ${route.ok ? route.country : `unreachable (budget ${providerBudget}ms)`}${route.ok && !changed ? " (unchanged)" : ""} ${route.elapsedMs}ms`);
        if (changed) return { ...route, changed: true };
        if (route.ok) {
          lastHealthy = route;
          if (Date.now() - startedProbingAt >= changeWindowMs) {
            trace("proxy_route_unchanged_accepted", {
              country: route.country,
              previousCountry,
              waitedMs: Date.now() - startedProbingAt
            }, `\u{1F6F0}\uFE0F  exit country stayed ${route.country}; continuing recovery`);
            return { ...route, changed: false };
          }
        }
        await sleep(800);
      }
      return lastHealthy ? { ...lastHealthy, changed: false } : null;
    } catch (error) {
      trace("isolated_route_probe_failed", { error: error.message });
      return lastHealthy ? { ...lastHealthy, changed: false } : null;
    } finally {
      if (targetId) {
        try {
          await cdp.send("Target.closeTarget", { targetId });
        } catch {
        }
      }
    }
  }
  async function benchmarkChatGPTRoute(stage) {
    const benchmark = await runChatGPTReadinessBenchmark(cdp, { timeoutMs: 18e3 });
    trace("chatgpt_readiness_benchmark", {
      stage,
      state: benchmark.state,
      ready: benchmark.ready,
      elapsedMs: benchmark.elapsedMs,
      fastest: benchmark.fastest?.name || null,
      probes: benchmark.results.map((result) => ({
        name: result.name,
        kind: result.kind,
        ok: result.ok,
        status: result.status,
        elapsedMs: result.elapsedMs,
        netError: result.netError,
        error: result.error
      }))
    }, `\u{1F9EA} ChatGPT readiness (${stage}): ${benchmark.state} in ${benchmark.elapsedMs}ms \u2014 ${benchmark.results.map(
      (result) => `${result.name}=${result.ok ? `HTTP ${result.status}` : result.error || "failed"}@${result.elapsedMs}ms`
    ).join(", ")}`);
    return benchmark;
  }
  async function waitForChatGPTDocument(minWaitMs = 1e4, sessionId = null, expectedMarker = null, options = {}) {
    const idleGraceMs = options.idleGraceMs ?? 12e3;
    const hardCapMs = options.hardCapMs ?? 24e4;
    const startedAt2 = Date.now();
    lastChatGPTActivityAt = Date.now();
    const shouldKeepWaiting = () => {
      const elapsed = Date.now() - startedAt2;
      if (elapsed >= hardCapMs) return false;
      if (elapsed < minWaitMs) return true;
      return Date.now() - lastChatGPTActivityAt < idleGraceMs;
    };
    while (shouldKeepWaiting() && cdp.isAlive()) {
      try {
        const res = await cdp.send("Runtime.evaluate", {
          expression: `JSON.stringify({
            host: location.hostname,
            href: location.href,
            ready: document.readyState,
            hasBody: !!document.body
          })`,
          returnByValue: true,
          timeout: 3e3
        }, sessionId);
        const page = JSON.parse(res.result?.value || "{}");
        const committed = !expectedMarker || String(page.href || "").includes(expectedMarker);
        if (page.host === "chatgpt.com" && page.hasBody && committed && (page.ready === "interactive" || page.ready === "complete")) return true;
      } catch {
      }
      await sleep(350);
    }
    return false;
  }
  async function verifyStableSession(sessionId = null) {
    const first = await probeChatGPTSession(sessionId);
    if (!first.loggedIn || first.status !== 200 || !/^[A-Z]{2}$/i.test(first.country || "")) {
      return { ok: false, state: first };
    }
    await sleep(900);
    const second = await probeChatGPTSession(sessionId);
    const stable = second.loggedIn && second.status === 200 && second.country === first.country;
    return { ok: stable, state: second, previousCountry: first.country };
  }
  async function currentPageLocation() {
    try {
      const res = await cdp.send("Runtime.evaluate", {
        expression: `JSON.stringify({host: location.hostname, path: location.pathname, hash: location.hash})`,
        returnByValue: true,
        timeout: 3e3
      });
      const page = JSON.parse(res.result?.value || "{}");
      const onChatGPT = page.host === "chatgpt.com";
      const path2 = typeof page.path === "string" && page.path.startsWith("/") ? page.path : "/";
      const hash = typeof page.hash === "string" && page.hash.startsWith("#") ? page.hash : "";
      return {
        onChatGPT,
        path: onChatGPT ? path2 : "/",
        hash: onChatGPT ? hash : ""
      };
    } catch {
      return { onChatGPT: false, path: "/", hash: "" };
    }
  }
  async function ensureSessionCookiePresent(stage) {
    const cookies = await readSessionCookies(cdp);
    if (cookies.length) return false;
    const stored = await installSessionCookie(cdp, sessionToken);
    trace("session_cookie_restored", {
      stage,
      stored,
      fingerprint: fingerprint(sessionToken)
    }, `\u{1F36A} session cookie ${stored ? "restored in the jar" : "left to header injection"} (${stage})`);
    return stored;
  }
  async function releaseOldRegionState(stage) {
    try {
      await cdp.send("Network.clearBrowserCache");
    } catch {
    }
    try {
      await cdp.send("Network.closeIdleConnections");
    } catch {
    }
    const includeChallenge = challengeSeenAt > 0 && Date.now() - challengeSeenAt < 6e4;
    const cleared = await clearRegionPinnedCookies(cdp, includeChallenge);
    trace(
      "region_pinned_cookies_cleared",
      { stage, count: cleared.length, includeChallenge },
      `\u{1F9F9} cleared ${cleared.length} IP-pinned cookie entries (${stage}${includeChallenge ? ", incl. challenge clearance" : ""})`
    );
    await ensureSessionCookiePresent(stage);
  }
  async function warmUpChatGPTInBackground(routeCountry, timeoutMs = 1e4) {
    let targetId = null;
    try {
      await ensureSessionCookiePresent("background warmup");
      const target = await cdp.send("Target.createTarget", {
        url: "about:blank",
        background: true
      });
      targetId = target.targetId;
      const attached = await cdp.send("Target.attachToTarget", {
        targetId,
        flatten: true
      });
      const sessionId = attached.sessionId;
      await cdp.send("Network.enable", {}, sessionId);
      await cdp.send("Page.enable", {}, sessionId);
      await cdp.send("Runtime.enable", {}, sessionId);
      try {
        await cdp.send("Network.setBypassServiceWorker", { bypass: true }, sessionId);
      } catch {
      }
      const warmupMarker = `proxy_warmup=${Date.now()}`;
      await cdp.send("Page.navigate", {
        url: `https://chatgpt.com/?${warmupMarker}`
      }, sessionId);
      const ready = await waitForChatGPTDocument(timeoutMs, sessionId, warmupMarker, { idleGraceMs: 15e3 });
      const state = ready ? await probeChatGPTSession(sessionId) : { status: 0 };
      trace("background_warmup", {
        ready,
        loggedIn: !!state.loggedIn,
        status: state.status || 0,
        country: state.country || null,
        routeCountry
      }, `\u{1F525} background warm-up ${ready ? "loaded" : "did not load"} (loggedIn=${!!state.loggedIn} me=${state.status || 0})`);
      return { ready, state };
    } catch (e) {
      trace("background_warmup_failed", { error: e.message, routeCountry });
      return { ready: false, state: { error: e.message } };
    } finally {
      if (targetId) {
        try {
          await cdp.send("Target.closeTarget", { targetId });
        } catch {
        }
      }
      await syncActiveSessionCookie("background warmup");
    }
  }
  async function reestablishSessionForNewIp(stage) {
    let refresh = { status: 0 };
    try {
      const res = await cdp.send("Runtime.evaluate", {
        expression: `(async () => {
          try {
            const response = await fetch('/api/auth/session?refresh=true&reason=integrity_state_mismatch', {
              credentials: 'include',
              cache: 'no-store'
            });
            const text = await response.text();
            let data = null;
            try { data = JSON.parse(text); } catch {}
            const account = typeof data?.account === 'string'
              ? data.account
              : (data?.account?.id || data?.account?.account_id || null);
            return JSON.stringify({
              status: response.status,
              loggedIn: !!data?.user,
              email: data?.user?.email || null,
              account,
              hasToken: !!data?.accessToken
            });
          } catch (e) {
            return JSON.stringify({ status: 0, error: e.message });
          }
        })()`,
        awaitPromise: true,
        returnByValue: true,
        timeout: 4e4
      }, null, 5e4);
      refresh = JSON.parse(res.result?.value || "{}");
    } catch (e) {
      refresh = { status: 0, error: e.message };
    }
    let accountCookieSet = false;
    let accountCookieExists = false;
    try {
      const jar = await cdp.send("Network.getCookies", { urls: ["https://chatgpt.com/"] });
      accountCookieExists = (jar.cookies || []).some((c) => c.name === "_account" && (c.value || "").length > 0);
    } catch {
    }
    if (refresh.account && !accountCookieExists) {
      try {
        const result = await cdp.send("Network.setCookie", {
          name: "_account",
          value: refresh.account,
          url: "https://chatgpt.com/",
          path: "/",
          secure: true,
          sameSite: "Lax"
        });
        accountCookieSet = result?.success !== false;
      } catch {
      }
    }
    await syncActiveSessionCookie("integrity refresh");
    trace("session_reestablished_for_ip", {
      stage,
      status: refresh.status || 0,
      loggedIn: !!refresh.loggedIn,
      hasToken: !!refresh.hasToken,
      accountKnown: !!refresh.account,
      accountCookieExists,
      accountCookieSet,
      error: refresh.error || null
    }, `\u{1F501} session refreshed for the new IP (${stage}): HTTP ${refresh.status || 0}${refresh.loggedIn ? ", signed in" : ""}${accountCookieSet ? ", active account restored" : accountCookieExists ? ", active account already set" : ""}`);
    return refresh;
  }
  async function detectAccountChooser() {
    try {
      const res = await cdp.send("Runtime.evaluate", {
        expression: `(() => {
          const body = (document.body?.innerText || '').toLowerCase();
          return body.includes('choose an account to continue') ||
            (body.includes('welcome back') && body.includes('log in to another account'));
        })()`,
        returnByValue: true,
        timeout: 5e3
      });
      return !!res.result?.value;
    } catch {
      return false;
    }
  }
  function repairAnonymousRender() {
    if (cleaning || !cdp.isAlive() || autoReloadInProgress) return Promise.resolve(false);
    if (anonymousRepairPromise) return anonymousRepairPromise;
    if (anonymousRepairAttempts >= 1) return Promise.resolve(false);
    if (Date.now() - lastAnonymousRepairAt < 6e4) return Promise.resolve(false);
    anonymousRepairPromise = (async () => {
      anonymousRepairAttempts++;
      lastAnonymousRepairAt = Date.now();
      const session = await probeChatGPTSession();
      if (!session.loggedIn) {
        const cause = session.challenge ? "Cloudflare returned a challenge instead of the session" : session.sessionStatus === 200 ? "ChatGPT reports no signed-in user, so the session token is no longer accepted" : `the session endpoint returned HTTP ${session.sessionStatus || 0}`;
        console.log("\n  \u{1F464} The page is signed out and reloading will not help.");
        console.log(`     Reason: ${cause}.`);
        if (!session.challenge && session.sessionStatus === 200) {
          console.log("     Refresh session_api.json with a new sessionToken to continue.\n");
        } else {
          console.log("");
        }
        trace("anonymous_render_session_invalid", {
          sessionStatus: session.sessionStatus || 0,
          meStatus: session.status || 0,
          challenge: !!session.challenge,
          cause
        }, `\u26A0\uFE0F signed-out page: ${cause}`);
        return false;
      }
      console.log("\n  \u{1F464} ChatGPT rendered the page as a signed-out visitor, but the session is still valid.");
      console.log("  \u{1F501} Re-establishing the session for the new IP, then reloading once...");
      trace("anonymous_render_repair_started", { country: session.country || null });
      cancelActiveQuestion();
      handled = false;
      lastOfferSeen = false;
      await reestablishSessionForNewIp("signed-out page");
      const marker = `session_repair=${Date.now()}`;
      try {
        await cdp.send("Page.stopLoading");
      } catch {
      }
      await ensureSessionCookiePresent("anonymous render repair");
      try {
        await cdp.send("Page.navigate", { url: `https://chatgpt.com/?${marker}` });
      } catch {
      }
      const ready = await waitForChatGPTDocument(8e3, null, marker);
      const beforeCheck = Date.now();
      await sleep(4e3);
      const stillAnonymous = anonymousRenderSeenAt > beforeCheck;
      const after = await probeChatGPTSession();
      trace("anonymous_render_repair_result", {
        documentReady: ready,
        stillAnonymous,
        loggedIn: !!after.loggedIn,
        country: after.country || null
      }, stillAnonymous ? "\u26A0\uFE0F the page is still rendering as signed out after the reload" : "\u2705 signed-in view restored");
      if (!stillAnonymous) {
        console.log(`  \u2705 Signed-in view restored${after.country ? ` (${after.country})` : ""}.
`);
        anonymousRepairAttempts = 0;
        anonymousNoticeShown = false;
        return true;
      }
      const chooser = await detectAccountChooser();
      if (!anonymousNoticeShown) {
        anonymousNoticeShown = true;
        console.log(chooser ? "  \u2139\uFE0F ChatGPT is asking you to pick the account again after the region change." : "  \u2139\uFE0F ChatGPT is keeping the signed-out view for this region.");
        console.log("     Your session is still valid, so the pay link works regardless.");
        console.log("     Click your account in the page if you also want the UI signed in.\n");
      }
      trace("anonymous_render_gave_up", { accountChooser: chooser, country: after.country || null });
      anonymousOfferPending = true;
      return false;
    })().finally(() => {
      anonymousRepairPromise = null;
    });
    return anonymousRepairPromise;
  }
  async function ensureChatGPTPageReady() {
    const page = await currentPageLocation();
    if (page.onChatGPT) {
      const probe2 = await probeChatGPTSession();
      if (probe2.loggedIn && probe2.status === 200) return true;
      trace("checkout_page_unhealthy", {
        loggedIn: !!probe2.loggedIn,
        status: probe2.status || 0,
        challenge: !!probe2.challenge
      }, `\u26A0\uFE0F page is on chatgpt.com but not usable (loggedIn=${!!probe2.loggedIn} me=${probe2.status || 0})`);
    }
    console.log("  \u{1F504} Restoring the ChatGPT page before requesting the pay link...");
    const marker = `checkout_ready=${Date.now()}`;
    try {
      await cdp.send("Page.navigate", { url: `https://chatgpt.com/?${marker}` });
    } catch {
    }
    if (!await waitForChatGPTDocument(8e3, null, marker)) {
      trace("checkout_page_recovery_failed", { stage: "document_not_ready" });
      return false;
    }
    const probe = await probeChatGPTSession();
    trace("checkout_page_ready", {
      loggedIn: !!probe.loggedIn,
      status: probe.status || 0,
      country: probe.country || null
    }, `\u{1F50D} checkout page ready: loggedIn=${!!probe.loggedIn} me=${probe.status || 0} country=${probe.country || "?"}`);
    return !!probe.loggedIn && probe.status === 200;
  }
  async function navigateOnceAndVerify(route, targetLocation, attempts = 3) {
    setRecoveryState("single-navigation");
    const page = `${targetLocation.path}${targetLocation.hash}`;
    let documentReady = false;
    let refreshMarker = "";
    for (let attempt = 1; attempt <= attempts && !documentReady; attempt++) {
      refreshMarker = `proxy_refresh=${Date.now()}`;
      const freshUrl = `https://chatgpt.com${targetLocation.path}?${refreshMarker}${targetLocation.hash}`;
      trace(
        "navigation_attempt",
        { attempt, attempts, routeCountry: route.country, url: freshUrl },
        `\u{1F504} loading ChatGPT on ${page} (attempt ${attempt}/${attempts})`
      );
      try {
        await cdp.send("Page.stopLoading");
      } catch {
      }
      if (attempt > 1) {
        try {
          await cdp.send("Page.navigate", { url: "about:blank" });
        } catch {
        }
        await sleep(400);
        try {
          await cdp.send("Network.closeIdleConnections");
        } catch {
        }
      }
      try {
        await cdp.send("Network.setCacheDisabled", { cacheDisabled: true });
      } catch {
      }
      await releaseOldRegionState(attempt === 1 ? "before_navigation" : `before_retry_${attempt}`);
      let navigationError = null;
      try {
        const result = await cdp.send("Page.navigate", { url: freshUrl }, null, 6e4);
        navigationError = result?.errorText || null;
      } catch (e) {
        navigationError = e.message;
      }
      if (navigationError) {
        trace(
          "navigation_error",
          { attempt, error: navigationError },
          `\u26A0\uFE0F navigation reported ${navigationError}`
        );
      }
      documentReady = await waitForChatGPTDocument(8e3 + (attempt - 1) * 4e3, null, refreshMarker);
      if (!documentReady) {
        trace(
          "navigation_timed_out",
          { attempt, routeCountry: route.country },
          `\u26A0\uFE0F attempt ${attempt} did not finish loading`
        );
      }
    }
    if (!documentReady) {
      console.log(`  \u26A0\uFE0F ChatGPT never finished loading through this route after ${attempts} attempts.`);
      console.log("     The proxy answers other sites, so it is likely throttling chatgpt.com.");
      console.log(`     Diagnostics: ${diagnosticsPath}
`);
      trace("recovery_failed", { stage: "document_not_ready", routeCountry: route.country, attempts });
      return null;
    }
    setRecoveryState("verifying");
    const verification = await verifyStableSession();
    const state = verification.state || {};
    trace("session_verification", {
      ok: verification.ok,
      loggedIn: !!state.loggedIn,
      status: state.status || 0,
      country: state.country || null,
      challenge: !!state.challenge,
      snippet: state.snippet || null,
      routeCountry: route.country,
      error: state.error || null
    }, `\u{1F50D} after refresh: loggedIn=${!!state.loggedIn} me=${state.status || 0} country=${state.country || "?"}${state.challenge ? " challenge=yes" : ""}`);
    if (verification.ok) {
      return acceptRecovery(state, route, "single_navigation");
    }
    const detail = state.challenge ? `Cloudflare returned a challenge page (session ${state.sessionStatus || "?"}, me ${state.status || "?"})` : !state.loggedIn ? `session endpoint is logged out (HTTP ${state.sessionStatus || 0})` : `country endpoint returned HTTP ${state.status || 0}`;
    console.log(`  \u26A0\uFE0F First refresh completed, but ${detail}.`);
    console.log(`     Diagnostics: ${diagnosticsPath}
`);
    trace("recovery_failed", { stage: "session_verification", detail });
    return null;
  }
  async function performRecovery(reasons) {
    setRecoveryState("probing-route");
    trace(
      "recovery_started",
      { reasons, knownCountry: lastKnownCountry },
      `\u{1F50C} recovery started: ${reasons.join(", ")}`
    );
    const routeChangeExpected = reasons.some((reason) => /ERR_|tunnel|connection|network/i.test(reason));
    const route = await waitForHealthyProxyRoute(
      lastKnownCountry,
      12e4,
      routeChangeExpected ? 6e3 : 2e3
    );
    if (!route) {
      console.log("  \u26A0\uFE0F The selected proxy never answered, even with escalating time budgets.");
      console.log(`     Diagnostics: ${diagnosticsPath}
`);
      trace("recovery_failed", { stage: "proxy_route_unreachable" });
      return null;
    }
    trace("proxy_route_ready", {
      country: route.country,
      provider: route.provider,
      changed: route.changed,
      elapsedMs: route.elapsedMs
    }, `\u2705 route ready: ${route.country} via ${route.provider} (${route.elapsedMs}ms${route.changed ? ", country changed" : ", country unchanged"})`);
    activeRouteCountry = route.country;
    const visibleLocation = await currentPageLocation();
    setRecoveryState("isolated-recovery-race");
    trace("isolated_recovery_race_start", {
      routeCountry: route.country,
      visiblePage: `${visibleLocation.path}${visibleLocation.hash}`
    }, `\u{1F3C1} racing isolated ChatGPT recovery paths for ${route.country}`);
    let winner;
    try {
      winner = await runIsolatedChatGPTRecovery(cdp, {
        expectedCountry: route.country,
        timeoutMs: 9e4,
        targetReadyTimeoutMs: 3e4,
        trace: (event, details) => trace(
          event,
          details,
          event === "recovery_race_winner" ? `\u{1F3C6} ${details.strategy} verified authenticated ${details.country} in ${details.elapsedMs}ms` : null
        )
      });
    } catch (error) {
      console.log(`  \u26A0\uFE0F No background strategy verified an authenticated ${route.country} session.`);
      console.log("     The visible page and rotating browser cookie were left untouched.");
      console.log(`     Diagnostics: ${diagnosticsPath}
`);
      trace("recovery_failed", {
        stage: "isolated_recovery_race",
        routeCountry: route.country,
        error: error.message
      });
      return null;
    }
    await syncActiveSessionCookie(`recovery race winner ${winner.strategy}`);
    await ensureSessionCookiePresent("after isolated recovery winner");
    trace("visible_navigation_released", {
      winner: winner.strategy,
      verifiedCountry: winner.verification.country,
      waitedMs: winner.elapsedMs
    }, `\u{1F513} background winner verified; visible navigation is now allowed`);
    return navigateOnceAndVerify(
      route,
      visibleLocation.onChatGPT ? visibleLocation : { path: "/", hash: "" },
      1
    );
  }
  function acceptRecovery(state, route, mode) {
    const oldCountry = lastKnownCountry;
    lastKnownCountry = state.country;
    activeRouteCountry = route.country;
    anonymousRepairAttempts = 0;
    anonymousNoticeShown = false;
    if (oldCountry && oldCountry !== state.country) {
      void reestablishSessionForNewIp("after recovery");
    }
    const matchesRoute = state.country === route.country;
    console.log(`  \u2705 Session active${state.email ? `: ${state.email}` : ""}`);
    if (matchesRoute) {
      console.log(`  \u{1F30D} Country: ${oldCountry || "?"} \u2192 ${state.country} (${currencyForCountry(state.country)})
`);
    } else {
      console.log(`  \u26A0\uFE0F ChatGPT still reports ${state.country} while the connection exits from ${route.country}.`);
      console.log(`     Checkout will use ${route.country} (${currencyForCountry(route.country)}); the plan cards may lag one refresh behind.
`);
    }
    trace("recovery_succeeded", {
      mode,
      chatgptCountry: state.country,
      routeCountry: route.country,
      matchesRoute
    });
    return state;
  }
  function requestProxyRecovery(reason) {
    if (cleaning || !cdp.isAlive()) return Promise.resolve(null);
    markActivity();
    pendingRecoveryReasons.add(reason);
    if (checkoutInProgress) {
      trace(
        "recovery_deferred_for_checkout",
        { reason },
        "\u21B7 visible-page recovery deferred while isolated checkout is active"
      );
      return Promise.resolve(null);
    }
    if (recoveryPromise) return recoveryPromise;
    cancelActiveQuestion();
    recoveryPromise = (async () => {
      autoReloadInProgress = true;
      handled = false;
      lastOfferSeen = false;
      const reasons = [...pendingRecoveryReasons];
      pendingRecoveryReasons.clear();
      return performRecovery(reasons);
    })().finally(async () => {
      try {
        await cdp.send("Network.setCacheDisabled", { cacheDisabled: false });
      } catch {
      }
      autoReloadInProgress = false;
      setRecoveryState("idle");
      recoveryPromise = null;
    });
    return recoveryPromise;
  }
  cdp.on("Network.responseReceived", async (params) => {
    if (autoReloadInProgress || cleaning) return;
    const url = params.response?.url || "";
    const status = params.response?.status;
    if (status !== 401 && status !== 403) return;
    if (!/chatgpt\.com\/(backend-api|api)\//i.test(url)) return;
    if (status === 403 && !/\/(backend-api\/me|api\/auth\/session)(?:[/?]|$)/i.test(url)) return;
    const now = Date.now();
    if (now - last401At < 5e3) return;
    last401At = now;
    const endpoint = url.replace(/^https?:\/\/[^/]+/, "");
    trace("auth_response_failure", {
      status,
      endpoint,
      remoteIP: params.response?.remoteIPAddress || null
    }, `\u26A1 auth failure ${status} on ${endpoint}`);
    void requestProxyRecovery(`${status} response`);
  });
  cdp.on("Network.loadingFailed", (params) => {
    if (autoReloadInProgress || cleaning || params.canceled) return;
    const error = params.errorText || "";
    const resourceType = params.type || "";
    const failedUrl = inFlight.get(params.requestId)?.url || null;
    inFlight.delete(params.requestId);
    const verdict = classifyRequestFailure({ errorText: error, type: resourceType, url: failedUrl });
    if (!verdict.relevant) {
      if (ROUTE_FAILURE_ERRORS.test(error)) {
        trace("transport_failure_ignored", {
          error,
          resourceType,
          url: failedUrl,
          host: verdict.host || null,
          reason: verdict.reason
        }, `\u21B7 ignoring ${error} from ${verdict.host || resourceType || "unknown"} (${verdict.reason})`);
      }
      return;
    }
    const now = Date.now();
    if (now - lastTransportFailureAt < 4e3) return;
    lastTransportFailureAt = now;
    trace("transport_failure", {
      error,
      resourceType,
      url: failedUrl,
      blockedReason: params.blockedReason || null
    }, `\u26A1 transport failure ${error} (${resourceType} ${shortenUrl(failedUrl || failedHost)})`);
    void requestProxyRecovery(error.replace(/^net::/, ""));
  });
  const countryMonitor = setInterval(async () => {
    if (!cdp.isAlive() || cleaning || autoReloadInProgress || !pageIsChatGPT) return;
    if (!inActivePeriod() && ++countryTick % 2) return;
    try {
      const res = await cdp.send("Runtime.evaluate", {
        expression: `fetch('/backend-api/me',{credentials:'include',cache:'no-store'}).then(async r=>{if(r.ok){const d=await r.json().catch(()=>({}));return JSON.stringify({status:200,country:d.country||null,email:d.email||null});}return JSON.stringify({status:r.status});}).catch(e=>JSON.stringify({status:0,error:e.message}))`,
        awaitPromise: true,
        returnByValue: true,
        timeout: 8e3
      });
      const data = JSON.parse(res.result?.value || "{}");
      trace("country_monitor", {
        status: data.status,
        country: data.country || null,
        knownCountry: lastKnownCountry,
        routeCountry: activeRouteCountry,
        error: data.error || null
      });
      if (data.status === 401 || data.status === 403) {
        markActivity();
        void requestProxyRecovery(`${data.status} from country check`);
        return;
      }
      if (data.country) {
        if (lastKnownCountry === null) {
          lastKnownCountry = data.country;
          trace(
            "country_established",
            { country: data.country },
            `\u{1F30D} ChatGPT country: ${data.country} (${currencyForCountry(data.country)})`
          );
        } else if (lastKnownCountry !== data.country) {
          markActivity();
          trace("country_changed_without_recovery", {
            from: lastKnownCountry,
            to: data.country
          }, `\u{1F504} ChatGPT country ${lastKnownCountry} \u2192 ${data.country} (${currencyForCountry(data.country)})`);
          lastKnownCountry = data.country;
          handled = false;
          lastOfferSeen = false;
          anonymousRepairAttempts = 0;
          anonymousNoticeShown = false;
          void reestablishSessionForNewIp("country change");
        }
      }
    } catch (e) {
      trace("country_monitor_error", { error: e.message });
    }
  }, 4e3);
  const snapshotMonitor = setInterval(async () => {
    if (!cdp.isAlive() || cleaning || autoReloadInProgress) return;
    if (!inActivePeriod() && ++snapshotTick % 2) return;
    const route = await probeExitCountry(cdp);
    const location = await currentPageLocation();
    if (route.ok && lastExitCountry && route.country !== lastExitCountry) {
      markActivity();
      trace(
        "exit_country_changed",
        { from: lastExitCountry, to: route.country },
        `\u{1F310} exit IP moved ${lastExitCountry} \u2192 ${route.country}; checking ChatGPT endpoints`
      );
      anonymousRepairAttempts = 0;
      anonymousNoticeShown = false;
      void requestProxyRecovery(`exit country changed ${lastExitCountry} \u2192 ${route.country}`);
    }
    if (route.ok) lastExitCountry = route.country;
    trace("snapshot", {
      exitCountry: route.ok ? route.country : null,
      exitProvider: route.provider || null,
      exitProbeMs: route.elapsedMs,
      chatgptCountry: lastKnownCountry,
      routeCountryUsedForCheckout: activeRouteCountry,
      cookieFingerprint: fingerprint(sessionToken),
      recoveryState,
      page: `${location.path}${location.hash}`,
      pausedRequests: pausedRequests.size
    }, `\u{1F4E1} exit=${route.ok ? route.country : "?"} chatgpt=${lastKnownCountry || "?"} cookie=${fingerprint(sessionToken)} page=${location.path}${location.hash} state=${recoveryState}`);
  }, 5e3);
  monitoringReady = true;
  console.log("  " + "\u2500".repeat(56));
  console.log("  \u{1F575}\uFE0F  MONITORING MODE ACTIVE");
  console.log("  \u{1F4CD} Watching for trial offers OR upgrade/plan surfaces");
  console.log("  \u26A1 Proxy recovery state machine: idle");
  console.log("  \u{1F30D} Country monitor 4s | exit-IP watch 5s (halved while idle)");
  console.log(`  \u{1F9FE} Trace: ${TRACE_ENABLED ? "every request, response, cookie, and page event" : "off"}`);
  console.log("  \u{1F4A1} Press Ctrl+C to quit at any time");
  console.log("  " + "\u2500".repeat(56) + "\n");
  while (cdp.isAlive() && !cleaning) {
    await sleep(2500);
    if (autoReloadInProgress) continue;
    if (handled && lastOfferSeen) {
      const cur = await detectOffer(cdp);
      if (!cur.found) {
        handled = false;
        lastOfferSeen = false;
      }
      continue;
    }
    let offer;
    if (anonymousOfferPending) {
      anonymousOfferPending = false;
      offer = { found: true, type: "noTrial", buttonLabel: "signed-out page", surface: "signed-out-page" };
    } else {
      offer = await detectOffer(cdp);
    }
    if (!offer.found) {
      lastOfferSeen = false;
      continue;
    }
    lastOfferSeen = true;
    if (handled) continue;
    const isTrial = offer.type === "trial";
    if (isTrial) {
      console.log("\n  \u{1F381} Trial offer detected on the page!");
      awaitingInput = true;
      var rawAnswer = await ask('  Skip trial and get direct Plus checkout?\n  Type "y" for YES, or press Enter for NO: ');
      awaitingInput = false;
    } else {
      const btnLabel = offer.buttonLabel || "Upgrade";
      const surfaceLabel = offer.surface === "signed-out-page" ? "signed-out page (session still valid)" : offer.surface === "upgrade-entry" ? "upgrade button" : offer.surface === "plan-dialog" ? "plan dialog" : offer.surface === "plan-cards" ? "plan cards" : "pricing page";
      console.log(`
  \u{1F4B3} "${btnLabel}" visible on the ${surfaceLabel}, so no free trial is offered`);
      awaitingInput = true;
      var rawAnswer = await ask('  Generate direct Plus pay link now?\n  Type "y" for YES, or press Enter for NO: ');
      awaitingInput = false;
    }
    if (rawAnswer === null) {
      console.log("  \u21BB Prompt canceled because the network changed; it will reappear after recovery.\n");
      handled = false;
      lastOfferSeen = false;
      continue;
    }
    const answer = rawAnswer.toLowerCase().trim();
    if (answer !== "y" && answer !== "yes") {
      console.log("  \u23ED\uFE0F  Skipped. Won't ask again for this page.\n");
      handled = true;
      continue;
    }
    console.log(`  \u23F3 Preparing isolated API checkout (Plus${isTrial ? ", no trial" : ""})...`);
    checkoutInProgress = true;
    try {
      const result = await runIsolatedCheckout({
        cdp,
        apiSession,
        currencyForCountry,
        trace,
        confirm: async (detected) => {
          const successful = detected.observations.filter((item) => item.ok && item.country).map((item) => `${item.provider}=${item.country}`).join(", ");
          console.log(`  \u{1F30D} Exit-country probes: ${successful || "none answered"}`);
          let country = detected.country || activeRouteCountry || null;
          if (!detected.reliable) {
            console.log(`  \u26A0\uFE0F Country quorum was not reached (${detected.agreeingProviders}/2 providers agree).`);
            awaitingInput = true;
            const manual = (await ask(
              `  Enter billing country (2-letter code${country ? `, Enter=${country}` : ""}): `
            ) || "").toUpperCase().trim();
            awaitingInput = false;
            if (/^[A-Z]{2}$/.test(manual)) country = manual;
          } else {
            console.log(`  \u2713 ${country} confirmed by ${detected.agreeingProviders} exit-IP providers.`);
          }
          if (!/^[A-Z]{2}$/.test(country || "")) return { confirmed: false };
          awaitingInput = true;
          const approval = await ask(
            `  Confirm Plus checkout for ${country} (${currencyForCountry(country)})? [y/N]: `
          );
          awaitingInput = false;
          return {
            confirmed: approval !== null && /^(y|yes)$/i.test(approval.trim()),
            country
          };
        }
      });
      if (result.cancelled) {
        console.log("  \u23ED\uFE0F  Checkout was not requested because country confirmation was declined.\n");
        handled = true;
        continue;
      }
      console.log(`  \u2705 Pay link generated!`);
      console.log(`     Country:  ${result.country}`);
      console.log(`     Currency: ${result.currency}`);
      console.log(`     Sentinel: ${result.sentinelUsed ? "YES" : "NO"}`);
      console.log(`     URL:      ${result.url}`);
      const savedTo = savePayLink(result, apiSession);
      if (savedTo) console.log(`     Saved:    ${savedTo}`);
      console.log("  \u{1F310} Opening in new tab...");
      await openInNewTab(cdp, result.url);
      console.log("  \u2705 Checkout is ready. Payment still requires your confirmation in the new tab.");
      const visible = await currentPageLocation();
      if (!visible.onChatGPT) {
        awaitingInput = true;
        const repair = await ask("  Visible ChatGPT tab is unhealthy. Try to repair it now? [y/N]: ");
        awaitingInput = false;
        if (repair !== null && /^(y|yes)$/i.test(repair.trim())) {
          await ensureChatGPTPageReady();
        }
      }
      console.log("");
      handled = true;
    } catch (e) {
      console.log(`  \u274C Failed: ${e.message}`);
      console.log("  \u21A9\uFE0F  Will retry if the offer reappears.\n");
      handled = true;
    } finally {
      checkoutInProgress = false;
    }
  }
  clearInterval(keepAlive);
  clearInterval(cookieSyncMonitor);
  clearInterval(countryMonitor);
  clearInterval(snapshotMonitor);
  if (typeof leakDetector !== "undefined") clearInterval(leakDetector);
  if (typeof statsReporter !== "undefined") clearInterval(statsReporter);
  try {
    ws.close();
  } catch {
  }
  cleanup();
  process.exit(0);
}
async function diagnosticMain() {
  console.log("\n" + "=".repeat(68));
  console.log("  \u{1F52C} ChatGPT Manual Diagnostic Recorder (passive mode)");
  console.log("=".repeat(68));
  console.log("  This mode does NOT inject cookies, reload pages, recover sessions,");
  console.log("  create checkout links, clear cache, or change browser state.\n");
  const runStamp = (/* @__PURE__ */ new Date()).toISOString().replace(/[:.]/g, "-");
  const diagnosticPath = path.resolve(`chatgpt_manual_diagnostic_${runStamp}.jsonl`);
  fs.writeFileSync(diagnosticPath, "");
  const started = Date.now();
  const emit = (event, details = {}, terminal = null) => {
    const record = {
      at: (/* @__PURE__ */ new Date()).toISOString(),
      sinceStartMs: Date.now() - started,
      event,
      ...details
    };
    try {
      fs.appendFileSync(diagnosticPath, JSON.stringify(record) + "\n");
    } catch {
    }
    if (terminal) console.log(`  ${((Date.now() - started) / 1e3).toFixed(3).padStart(9)}s ${terminal}`);
  };
  const cookieSummary = (raw) => {
    const values = String(raw || "").split(/;\s*/).filter(Boolean);
    return values.map((item) => {
      const eq = item.indexOf("=");
      const name = eq >= 0 ? item.slice(0, eq).trim() : item.trim();
      const value = eq >= 0 ? item.slice(eq + 1) : "";
      return { name, bytes: value.length, fingerprint: fingerprint(value) };
    });
  };
  const setCookieSummary = (raw) => String(raw || "").split("\n").filter(Boolean).map((line) => {
    const pair = line.split(";")[0];
    const eq = pair.indexOf("=");
    const name = eq >= 0 ? pair.slice(0, eq).trim() : pair.trim();
    const value = eq >= 0 ? pair.slice(eq + 1) : "";
    return { name, bytes: value.length, fingerprint: fingerprint(value), clears: value.length === 0 };
  });
  const sensitiveQueryName = /token|auth|verify|signature|sig|secret|password|session|jwt|code|state|key|credential|ticket/i;
  const safeUrl2 = (raw) => {
    try {
      const parsed = new URL(String(raw || ""));
      parsed.pathname = parsed.pathname.split("/").map(
        (segment) => segment.length > 120 ? `<redacted-path:${segment.length}b:${fingerprint(segment)}>` : segment
      ).join("/");
      for (const [name, value] of [...parsed.searchParams.entries()]) {
        if (sensitiveQueryName.test(name) || /(?:^|_)(?:tk|nonce)(?:$|_)/i.test(name) || value.length > 80) {
          parsed.searchParams.set(name, `<redacted:${value.length}b:${fingerprint(value)}>`);
        }
      }
      return parsed.toString();
    } catch {
      return String(raw || "").slice(0, 1e3);
    }
  };
  const safeHeaders = (headers) => {
    const result = {};
    for (const [name, value] of Object.entries(headers || {})) {
      const lower = name.toLowerCase();
      if (lower === "cookie") result[name] = cookieSummary(value);
      else if (lower === "set-cookie") result[name] = setCookieSummary(value);
      else if (lower === "authorization" || lower === "proxy-authorization") {
        result[name] = `<redacted ${String(value).length}b fp=${fingerprint(value)}>`;
      } else if (lower === "referer" || lower === "location") result[name] = safeUrl2(value);
      else result[name] = String(value).slice(0, 500);
    }
    return result;
  };
  console.log(`  \u{1F5C2}\uFE0F  Recording everything to: ${diagnosticPath}`);
  console.log("  1) Log in to ChatGPT manually");
  console.log("  2) Install/connect the proxy extension in another tab");
  console.log("  3) Return to ChatGPT and press Refresh once");
  console.log("  4) Wait until the result is visible, then press Ctrl+C here\n");
  const chromePath = findChrome();
  if (!chromePath) throw new Error("Chrome not found");
  const userDir = path.join(os.tmpdir(), `chatgpt_manual_diagnostic_${Date.now()}`);
  fs.mkdirSync(userDir, { recursive: true });
  const cdpPort = 9600 + Math.floor(Math.random() * 250);
  const chrome = spawn(chromePath, [
    `--remote-debugging-port=${cdpPort}`,
    `--user-data-dir=${userDir}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--start-maximized",
    "about:blank"
  ], { stdio: "ignore" });
  let ws = null;
  let stopping = false;
  const intervals = [];
  const stop = () => {
    if (stopping) return;
    stopping = true;
    for (const timer of intervals) clearInterval(timer);
    try {
      ws?.close();
    } catch {
    }
    try {
      chrome.kill();
    } catch {
    }
    setTimeout(() => {
      try {
        fs.rmSync(userDir, { recursive: true, force: true });
      } catch {
      }
    }, 800);
    console.log(`
  \u2705 Diagnostic recording saved: ${diagnosticPath}`);
  };
  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, () => {
      stop();
      process.exit(0);
    });
  }
  ws = await connectCDP(cdpPort);
  if (!ws) throw new Error("Could not attach to Chrome");
  const cdp = cdpClient(ws);
  await cdp.send("Network.enable", {
    maxTotalBufferSize: 100 * 1024 * 1024,
    maxResourceBufferSize: 10 * 1024 * 1024,
    maxPostDataSize: 1024 * 1024
  });
  await cdp.send("Page.enable");
  await cdp.send("Runtime.enable");
  try {
    await cdp.send("Log.enable");
  } catch {
  }
  try {
    await cdp.send("Security.enable");
  } catch {
  }
  emit("diagnostic_started", { cdpPort, passive: true }, "\u2705 passive recorder attached \u2014 perform the manual steps now");
  const requests = /* @__PURE__ */ new Map();
  const importantResponses = /* @__PURE__ */ new Map();
  let lastCookieState = "";
  let lastPageState = "";
  cdp.on("Network.requestWillBeSent", (params) => {
    const req = params.request || {};
    const entry = {
      requestId: params.requestId,
      url: safeUrl2(req.url),
      method: req.method,
      type: params.type || "Other",
      documentURL: params.documentURL ? safeUrl2(params.documentURL) : null,
      initiator: params.initiator?.type || null,
      startedAt: Date.now(),
      headers: safeHeaders(req.headers),
      postBytes: req.postData ? Buffer.byteLength(req.postData) : 0,
      redirectFrom: params.redirectResponse?.url ? safeUrl2(params.redirectResponse.url) : null,
      redirectStatus: params.redirectResponse?.status || null
    };
    requests.set(params.requestId, entry);
    emit("request", entry, `\u2192 ${entry.method} ${entry.type} ${shortenUrl(entry.url)}`);
  });
  cdp.on("Network.requestWillBeSentExtraInfo", (params) => {
    const blockedCookies = (params.associatedCookies || []).filter((item) => (item.blockedReasons || []).length).map((item) => ({ name: item.cookie?.name, reasons: item.blockedReasons }));
    emit("request_extra", {
      requestId: params.requestId,
      headers: safeHeaders(params.headers),
      blockedCookies,
      connectTiming: params.connectTiming || null,
      clientSecurityState: params.clientSecurityState || null
    }, blockedCookies.length ? `\u{1F6AB} blocked request cookies: ${blockedCookies.map((x) => x.name).join(", ")}` : null);
  });
  cdp.on("Network.responseReceived", (params) => {
    const response = params.response || {};
    const request = requests.get(params.requestId);
    const durationMs = request ? Date.now() - request.startedAt : null;
    const timing = response.timing || null;
    const record = {
      requestId: params.requestId,
      url: response.url,
      type: params.type || request?.type || "Other",
      status: response.status,
      statusText: response.statusText,
      mimeType: response.mimeType,
      remoteIP: response.remoteIPAddress || null,
      remotePort: response.remotePort || null,
      protocol: response.protocol || null,
      alternateProtocolUsage: response.alternateProtocolUsage || null,
      connectionId: response.connectionId ?? null,
      connectionReused: response.connectionReused ?? null,
      fromDiskCache: !!response.fromDiskCache,
      fromServiceWorker: !!response.fromServiceWorker,
      fromPrefetchCache: !!response.fromPrefetchCache,
      durationMs,
      timing,
      phaseMs: timing ? {
        proxy: timing.proxyStart >= 0 && timing.proxyEnd >= 0 ? timing.proxyEnd - timing.proxyStart : null,
        dns: timing.dnsStart >= 0 && timing.dnsEnd >= 0 ? timing.dnsEnd - timing.dnsStart : null,
        connect: timing.connectStart >= 0 && timing.connectEnd >= 0 ? timing.connectEnd - timing.connectStart : null,
        ssl: timing.sslStart >= 0 && timing.sslEnd >= 0 ? timing.sslEnd - timing.sslStart : null,
        ttfb: timing.receiveHeadersStart >= 0 ? timing.receiveHeadersStart : null
      } : null,
      headers: safeHeaders(response.headers),
      securityDetails: response.securityDetails ? {
        protocol: response.securityDetails.protocol,
        keyExchange: response.securityDetails.keyExchange,
        cipher: response.securityDetails.cipher,
        certificateId: response.securityDetails.certificateId,
        issuer: response.securityDetails.issuer,
        validFrom: response.securityDetails.validFrom,
        validTo: response.securityDetails.validTo
      } : null
    };
    emit(
      "response",
      record,
      `\u2190 ${response.status} ${record.type} ${shortenUrl(response.url)} ip=${record.remoteIP || "?"} ${durationMs ?? "?"}ms`
    );
    if (/chatgpt\.com\/(api\/auth\/session|backend-(?:api|anon)\/me|backend-api\/checkout_pricing_config)/i.test(response.url || "")) {
      importantResponses.set(params.requestId, { url: response.url, status: response.status });
    }
  });
  cdp.on("Network.responseReceivedExtraInfo", (params) => {
    const rawSetCookie = params.headers?.["set-cookie"] || params.headers?.["Set-Cookie"] || "";
    const cookies = setCookieSummary(rawSetCookie);
    const blockedCookies = (params.blockedCookies || []).map((item) => ({
      name: item.cookie?.name || item.cookieLine?.split("=")[0],
      reasons: item.blockedReasons
    }));
    emit("response_extra", {
      requestId: params.requestId,
      statusCode: params.statusCode,
      headers: safeHeaders(params.headers),
      setCookies: cookies,
      blockedCookies,
      resourceIPAddressSpace: params.resourceIPAddressSpace || null
    }, cookies.length ? `\u{1F36A} Set-Cookie: ${cookies.map((c) => `${c.name}[${c.fingerprint}${c.clears ? ":clear" : ""}]`).join(", ")}` : blockedCookies.length ? `\u{1F6AB} blocked response cookies: ${blockedCookies.map((x) => x.name).join(", ")}` : null);
  });
  cdp.on("Network.loadingFinished", async (params) => {
    const important = importantResponses.get(params.requestId);
    requests.delete(params.requestId);
    if (!important) return;
    importantResponses.delete(params.requestId);
    try {
      const bodyResult = await cdp.send("Network.getResponseBody", { requestId: params.requestId });
      let body = bodyResult.base64Encoded ? Buffer.from(bodyResult.body, "base64").toString("utf8") : bodyResult.body;
      let parsed = null;
      try {
        parsed = JSON.parse(body);
      } catch {
      }
      let verdict;
      if (/api\/auth\/session/i.test(important.url)) {
        verdict = parsed ? {
          loggedIn: !!parsed.user,
          emailFingerprint: fingerprint(parsed.user?.email || ""),
          hasAccessToken: !!parsed.accessToken,
          accountFingerprint: fingerprint(
            typeof parsed.account === "string" ? parsed.account : parsed.account?.id || parsed.account?.account_id || ""
          ),
          expires: parsed.expires || null
        } : { json: false, snippet: String(body).slice(0, 120) };
      } else if (/backend-(?:api|anon)\/me/i.test(important.url)) {
        verdict = parsed ? {
          country: parsed.country || null,
          emailFingerprint: fingerprint(parsed.email || parsed.user?.email || ""),
          authenticated: important.url.includes("/backend-api/"),
          keys: Object.keys(parsed).sort()
        } : { json: false, snippet: String(body).slice(0, 120) };
      } else {
        verdict = parsed ? {
          countryFromUrl: (important.url.match(/configs\/([A-Z]{2})/i) || [])[1] || null,
          keys: Object.keys(parsed).sort()
        } : { json: false, snippet: String(body).slice(0, 120) };
      }
      emit("important_response_body", {
        requestId: params.requestId,
        url: important.url,
        status: important.status,
        verdict
      }, `\u{1F50E} ${shortenUrl(important.url)} => ${JSON.stringify(verdict)}`);
    } catch (e) {
      emit("response_body_unavailable", { requestId: params.requestId, url: important.url, error: e.message });
    }
  });
  cdp.on("Network.loadingFailed", (params) => {
    const request = requests.get(params.requestId);
    requests.delete(params.requestId);
    importantResponses.delete(params.requestId);
    emit("loading_failed", {
      requestId: params.requestId,
      url: request?.url || null,
      type: params.type || request?.type || null,
      errorText: params.errorText,
      canceled: !!params.canceled,
      blockedReason: params.blockedReason || null,
      corsErrorStatus: params.corsErrorStatus || null,
      durationMs: request ? Date.now() - request.startedAt : null
    }, `\u26A1 FAILED ${params.errorText} ${shortenUrl(request?.url || "")}`);
  });
  cdp.on("Page.frameNavigated", (params) => {
    if (params.frame?.parentId) return;
    emit("main_frame_navigated", {
      url: params.frame?.url,
      unreachableUrl: params.frame?.unreachableUrl ? safeUrl2(params.frame.unreachableUrl) : null,
      securityOrigin: params.frame?.securityOrigin || null,
      mimeType: params.frame?.mimeType || null
    }, `\u{1F9ED} ${params.frame?.unreachableUrl ? `ERROR ${params.frame.unreachableUrl}` : params.frame?.url}`);
  });
  cdp.on("Page.domContentEventFired", (params) => emit("dom_content_loaded", params, "\u{1F4C4} DOMContentLoaded"));
  cdp.on("Page.loadEventFired", (params) => emit("page_load", params, "\u{1F4C4} load"));
  cdp.on("Runtime.exceptionThrown", (params) => {
    const details = params.exceptionDetails || {};
    emit("page_exception", {
      text: details.text,
      description: details.exception?.description?.slice(0, 1e3) || null,
      url: details.url ? safeUrl2(details.url) : null,
      lineNumber: details.lineNumber,
      columnNumber: details.columnNumber
    }, `\u{1F4A5} ${String(details.exception?.description || details.text || "").split("\n")[0].slice(0, 180)}`);
  });
  cdp.on("Runtime.consoleAPICalled", (params) => {
    if (!["error", "warning", "assert"].includes(params.type)) return;
    const text = (params.args || []).map((arg) => arg.value ?? arg.description ?? arg.type).join(" ").slice(0, 1e3);
    emit("page_console", { level: params.type, text }, `\u{1F5A5}\uFE0F console.${params.type}: ${text.slice(0, 180)}`);
  });
  cdp.on("Log.entryAdded", (params) => {
    const entry = params.entry || {};
    emit("browser_log", {
      level: entry.level,
      source: entry.source,
      text: entry.text,
      url: entry.url ? safeUrl2(entry.url) : null,
      networkRequestId: entry.networkRequestId || null
    }, ["error", "warning"].includes(entry.level) ? `\u{1F4D5} ${entry.source}/${entry.level}: ${String(entry.text).slice(0, 180)}` : null);
  });
  cdp.on("Security.securityStateChanged", (params) => {
    emit("security_state", {
      securityState: params.securityState,
      schemeIsCryptographic: params.schemeIsCryptographic,
      explanations: params.explanations,
      insecureContentStatus: params.insecureContentStatus
    }, `\u{1F510} security=${params.securityState}`);
  });
  intervals.push(setInterval(async () => {
    if (!cdp.isAlive() || stopping) return;
    try {
      const result = await cdp.send("Network.getCookies", { urls: ["https://chatgpt.com/"] });
      const relevant = (result.cookies || []).filter((c) => /^(?:__Secure-next-auth\.session-token(?:\.\d+)?|_account|__cf_bm|_cfuvid|cf_clearance|__oailb|__cflb|oai-)/.test(c.name)).map((c) => ({
        name: c.name,
        domain: c.domain,
        path: c.path,
        bytes: (c.value || "").length,
        fingerprint: fingerprint(c.value || ""),
        expires: c.expires,
        httpOnly: c.httpOnly,
        secure: c.secure,
        sameSite: c.sameSite || null,
        session: c.session,
        priority: c.priority || null,
        partitionKey: c.partitionKey || null
      })).sort((a, b) => `${a.name}@${a.domain}`.localeCompare(`${b.name}@${b.domain}`));
      const state = JSON.stringify(relevant);
      if (state !== lastCookieState) {
        lastCookieState = state;
        emit(
          "cookie_jar_changed",
          { cookies: relevant },
          `\u{1FAD9} cookie jar: ${relevant.map((c) => `${c.name}[${c.fingerprint}]`).join(", ") || "empty"}`
        );
      }
    } catch (e) {
      emit("cookie_snapshot_failed", { error: e.message });
    }
  }, 750));
  intervals.push(setInterval(async () => {
    if (!cdp.isAlive() || stopping) return;
    try {
      const result = await cdp.send("Runtime.evaluate", {
        expression: `JSON.stringify({
          href: location.href,
          host: location.hostname,
          readyState: document.readyState,
          title: document.title,
          visible: document.visibilityState,
          online: navigator.onLine,
          accountChooser: (document.body?.innerText || '').toLowerCase().includes('choose an account to continue'),
          hasLoginButton: Array.from(document.querySelectorAll('button,a')).some(el => /^(log in|login)$/i.test((el.textContent || '').trim())),
          hasUpgrade: Array.from(document.querySelectorAll('button,a,span')).some(el => /upgrade/i.test((el.textContent || '').trim()))
        })`,
        returnByValue: true,
        timeout: 3e3
      });
      const page = JSON.parse(result.result?.value || "{}");
      const state = JSON.stringify(page);
      if (state !== lastPageState) {
        lastPageState = state;
        emit(
          "page_state_changed",
          page,
          `\u{1F441}\uFE0F page=${shortenUrl(page.href || "")} ready=${page.readyState} chooser=${!!page.accountChooser} login=${!!page.hasLoginButton} upgrade=${!!page.hasUpgrade}`
        );
      }
    } catch (e) {
      emit("page_state_unavailable", { error: e.message });
    }
  }, 750));
  emit("initial_navigation_requested", { url: "https://chatgpt.com/" }, "\u{1F310} opening ChatGPT under observation");
  await cdp.send("Page.navigate", { url: "https://chatgpt.com/" });
  while (cdp.isAlive() && !stopping) await sleep(1e3);
  stop();
}
async function loginMain() {
  console.log("\n" + "=".repeat(68));
  console.log("  \u{1F510} ChatGPT Manual Login (stores the complete session in a profile)");
  console.log("=".repeat(68));
  console.log("  Nothing is injected here. Sign in exactly like a normal browser so");
  console.log("  ChatGPT stores its full cookie family, not just the NextAuth token.\n");
  console.log(`  \u{1F4C1} Profile: ${PROFILE_DIR}`);
  const chromePath = findChrome();
  if (!chromePath) throw new Error("Chrome not found");
  fs.mkdirSync(path.join(PROFILE_DIR, "Default"), { recursive: true });
  const cdpPort = 9300 + Math.floor(Math.random() * 250);
  const chrome = spawn(chromePath, [
    `--remote-debugging-port=${cdpPort}`,
    `--user-data-dir=${PROFILE_DIR}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--start-maximized",
    "https://chatgpt.com/"
  ], { stdio: "ignore" });
  let stopping = false;
  const timers = [];
  let loginCdp = null;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    for (const timer of timers) clearInterval(timer);
    await closeBrowserGracefully(loginCdp, chrome);
    console.log("\n  \u2705 Login state saved in the profile. Start the tool with:");
    console.log("     node chatgpt_smart\n");
  };
  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, () => {
      void stop().then(() => process.exit(0));
    });
  }
  const ws = await connectCDP(cdpPort);
  if (!ws) throw new Error("Could not attach to Chrome");
  const cdp = cdpClient(ws);
  loginCdp = cdp;
  await cdp.send("Network.enable");
  console.log("  \u25B6 Sign in, then press Ctrl+C here once you see your account.\n");
  const authCookiePattern = /^(?:__Secure-next-auth\.session-token|_account|unified_session_manifest|usc_|oai-client-auth-|auth-session-minimized|login_session|auth_provider)/;
  let lastSummary = "";
  timers.push(setInterval(async () => {
    if (stopping || !cdp.isAlive()) return;
    try {
      const result = await cdp.send("Network.getCookies", {
        urls: ["https://chatgpt.com/", "https://auth.openai.com/"]
      });
      const relevant = (result.cookies || []).filter((cookie) => authCookiePattern.test(cookie.name)).map((cookie) => cookie.name).sort();
      const summary = relevant.join(",");
      if (summary && summary !== lastSummary) {
        lastSummary = summary;
        const hasSession = relevant.some((name) => name.startsWith("__Secure-next-auth.session-token"));
        const hasAccount = relevant.includes("_account");
        const hasManifest = relevant.some((name) => name.startsWith("unified_session_manifest") || name.startsWith("usc_"));
        console.log(`  \u{1F36A} stored: ${relevant.join(", ")}`);
        if (hasSession && hasAccount && hasManifest) {
          console.log("  \u2705 Full login state detected (session + account + unified manifest).");
          console.log("     This is what survives a proxy country change. Press Ctrl+C to finish.");
        }
      }
    } catch {
    }
  }, 1e3));
  while (cdp.isAlive() && !stopping) await sleep(1e3);
  await stop();
}
var selectedMain = cliFlag("diagnose") ? diagnosticMain : cliFlag("login") ? loginMain : main;
selectedMain().catch((e) => {
  console.error("\n\u274C Fatal:", e.stack || e.message);
  process.exit(1);
});
