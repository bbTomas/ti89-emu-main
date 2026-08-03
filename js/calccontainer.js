(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
        typeof define === 'function' && define.amd ? define(['exports'], factory) :
            (factory((global.async = {})));
}(this, (function (exports) { 'use strict';
    function callbacksFor(object) {
        var callbacks = object._promiseCallbacks;
        if (!callbacks) {
            callbacks = object._promiseCallbacks = {};
        }
        return callbacks;
    }
    var EventTarget = {
        mixin: function (object) {
            object.trigger = this.trigger;
            object._promiseCallbacks = undefined;
            return object;
        },
        trigger: function (eventName, options, label) {
            var allCallbacks = callbacksFor(this);
            var callbacks = allCallbacks[eventName];
            if (callbacks) {
                var callback = void 0;
                for (var i = 0; i < callbacks.length; i++) {
                    callback = callbacks[i];
                    callback(options, label);
                }
            }
        }
    };
    var config = {
        instrument: false
    };
    EventTarget['mixin'](config);
    var queue = [];
    function scheduleFlush() {
        setTimeout(function () {
            for (var i = 0; i < queue.length; i++) {
                var entry = queue[i];
                var payload = entry.payload;
                payload.guid = payload.key + payload.id;
                payload.childGuid = payload.key + payload.childId;
                if (payload.error) {
                    payload.stack = payload.error.stack;
                }
                config['trigger'](entry.name, entry.payload);
            }
            queue.length = 0;
        }, 50);
    }
    function instrument(eventName, promise, child) {
        if (1 === queue.push({
            name: eventName,
            payload: {
                key: promise._guidKey,
                id: promise._id,
                eventName: eventName,
                detail: promise._result,
                childId: child && child._id,
                label: promise._label,
                timeStamp: Date.now(),
                error: config["instrument-with-stack"] ? new Error(promise._label) : null
            } })) {
            scheduleFlush();
        }
    }
    function resolve$$1(object, label) {
        var Constructor = this;
        if (object && typeof object === 'object' && object.constructor === Constructor) {
            return object;
        }
        var promise = new Constructor(noop, label);
        resolve$1(promise, object);
        return promise;
    }
    function withOwnPromise() {
    }
    function objectOrFunction(x) {
        var type = typeof x;
        return x !== null && (type === 'object' || type === 'function');
    }
    function noop() {}
    var PENDING = void 0;
    var FULFILLED = 1;
    var REJECTED = 2;
    var TRY_CATCH_ERROR = { error: null };
    function getThen(promise) {
        try {
            return promise.then;
        } catch (error) {
            TRY_CATCH_ERROR.error = error;
            return TRY_CATCH_ERROR;
        }
    }
    var tryCatchCallback = void 0;
    function tryCatcher() {
        try {
            var target = tryCatchCallback;
            tryCatchCallback = null;
            return target.apply(this, arguments);
        } catch (e) {
            TRY_CATCH_ERROR.error = e;
            return TRY_CATCH_ERROR;
        }
    }
    function tryCatch(fn) {
        tryCatchCallback = fn;
        return tryCatcher;
    }
    function handleForeignThenable(promise, thenable, then$$1) {
        config.casync(function (promise) {
            var sealed = false;
            var result = tryCatch(then$$1).call(thenable, function (value) {
                if (sealed) {
                    return;
                }
                sealed = true;
                if (thenable === value) {
                    fulfill(promise, value);
                } else {
                    resolve$1(promise, value);
                }
            }, function (reason) {
                if (sealed) {
                    return;
                }
                sealed = true;
                reject(promise, reason);
            }, 'Settle: ' + (promise._label || ' unknown promise'));
            if (!sealed && result === TRY_CATCH_ERROR) {
                sealed = true;
                var error = TRY_CATCH_ERROR.error;
                TRY_CATCH_ERROR.error = null;
                reject(promise, error);
            }
        }, promise);
    }
    function handleOwnThenable(promise, thenable) {
        if (thenable._state === FULFILLED) {
            fulfill(promise, thenable._result);
        } else if (thenable._state === REJECTED) {
            thenable._onError = null;
            reject(promise, thenable._result);
        } else {
            subscribe(thenable, undefined, function (value) {
                if (thenable === value) {
                    fulfill(promise, value);
                } else {
                    resolve$1(promise, value);
                }
            }, function (reason) {
                return reject(promise, reason);
            });
        }
    }
    function handleMaybeThenable(promise, maybeThenable, then$$1) {
        var isOwnThenable = maybeThenable.constructor === promise.constructor && then$$1 === then && promise.constructor.resolve === resolve$$1;
        if (isOwnThenable) {
            handleOwnThenable(promise, maybeThenable);
        } else if (then$$1 === TRY_CATCH_ERROR) {
            var error = TRY_CATCH_ERROR.error;
            TRY_CATCH_ERROR.error = null;
            reject(promise, error);
        } else if (typeof then$$1 === 'function') {
            handleForeignThenable(promise, maybeThenable, then$$1);
        } else {
            fulfill(promise, maybeThenable);
        }
    }
    function resolve$1(promise, value) {
        if (promise === value) {
            fulfill(promise, value);
        } else if (objectOrFunction(value)) {
            handleMaybeThenable(promise, value, getThen(value));
        } else {
            fulfill(promise, value);
        }
    }
    function publishRejection(promise) {
        if (promise._onError) {
            promise._onError(promise._result);
        }
        publish(promise);
    }
    function fulfill(promise, value) {
        if (promise._state !== PENDING) {
            return;
        }
        promise._result = value;
        promise._state = FULFILLED;
        if (promise._subscribers.length === 0) {
            if (config.instrument) {
                instrument('fulfilled', promise);
            }
        } else {
            config.casync(publish, promise);
        }
    }
    function reject(promise, reason) {
        if (promise._state !== PENDING) {
            return;
        }
        promise._state = REJECTED;
        promise._result = reason;
        config.casync(publishRejection, promise);
    }
    function subscribe(parent, child, onFulfillment, onRejection) {
        var subscribers = parent._subscribers;
        var length = subscribers.length;
        parent._onError = null;
        subscribers[length] = child;
        subscribers[length + FULFILLED] = onFulfillment;
        subscribers[length + REJECTED] = onRejection;
        if (length === 0 && parent._state) {
            config.casync(publish, parent);
        }
    }
    function publish(promise) {
        var subscribers = promise._subscribers;
        var settled = promise._state;
        if (config.instrument) {
            instrument(settled === FULFILLED ? 'fulfilled' : 'rejected', promise);
        }
        if (subscribers.length === 0) {
            return;
        }
        var child = void 0,
            callback = void 0,
            result = promise._result;
        for (var i = 0; i < subscribers.length; i += 3) {
            child = subscribers[i];
            callback = subscribers[i + settled];
            if (child) {
                invokeCallback(settled, child, callback, result);
            } else {
                callback(result);
            }
        }
        promise._subscribers.length = 0;
    }
    function invokeCallback(state, promise, callback, result) {
        var hasCallback = typeof callback === 'function';
        var value = void 0;
        if (hasCallback) {
            value = tryCatch(callback)(result);
        } else {
            value = result;
        }
        if (promise._state !== PENDING) {
        } else if (value === promise) {
            reject(promise, withOwnPromise());
        } else if (value === TRY_CATCH_ERROR) {
            var error = TRY_CATCH_ERROR.error;
            TRY_CATCH_ERROR.error = null;
            reject(promise, error);
        } else if (hasCallback) {
            resolve$1(promise, value);
        } else if (state === FULFILLED) {
            fulfill(promise, value);
        } else if (state === REJECTED) {
            reject(promise, value);
        }
    }
    function initializePromise(promise, resolver) {
        var resolved = false;
        try {
            resolver(function (value) {
                if (resolved) {
                    return;
                }
                resolved = true;
                resolve$1(promise, value);
            }, function (reason) {
                if (resolved) {
                    return;
                }
                resolved = true;
                reject(promise, reason);
            });
        } catch (e) {
            reject(promise, e);
        }
    }
    function then(onFulfillment, onRejection, label) {
        var parent = this;
        var state = parent._state;

        if (state === FULFILLED && !onFulfillment || state === REJECTED && !onRejection) {
            config.instrument && instrument('chained', parent, parent);
            return parent;
        }
        parent._onError = null;
        var child = new parent.constructor(noop, label);
        var result = parent._result;
        config.instrument && instrument('chained', parent, child);
        if (state === PENDING) {
            subscribe(parent, child, onFulfillment, onRejection);
        } else {
            var callback = state === FULFILLED ? onFulfillment : onRejection;
            config.casync(function () {
                return invokeCallback(state, child, callback, result);
            });
        }
        return child;
    }
    var Enumerator = function () {
        function Enumerator(Constructor, input, abortOnReject, label) {
            this._instanceConstructor = Constructor;
            this.promise = new Constructor(noop, label);
            this._abortOnReject = abortOnReject;
            this._isUsingOwnPromise = Constructor === Promise;
            this._isUsingOwnResolve = Constructor.resolve === resolve$$1;
            this._init.apply(this, arguments);
        }
        Enumerator.prototype._init = function _init(Constructor, input) {
            var len = input.length || 0;
            this.length = len;
            this._remaining = len;
            this._result = new Array(len);
            this._enumerate(input);
        };
        Enumerator.prototype._enumerate = function _enumerate(input) {
            var length = this.length;
            var promise = this.promise;
            for (var i = 0; promise._state === PENDING && i < length; i++) {
                this._eachEntry(input[i], i, true);
            }
            this._checkFullfillment();
        };
        Enumerator.prototype._checkFullfillment = function _checkFullfillment() {
            if (this._remaining === 0) {
                var result = this._result;
                fulfill(this.promise, result);
                this._result = null;
            }
        };
        Enumerator.prototype._settleMaybeThenable = function _settleMaybeThenable(entry, i, firstPass) {
            var c = this._instanceConstructor;
            if (this._isUsingOwnResolve) {
                var then$$1 = getThen(entry);
                if (then$$1 === then && entry._state !== PENDING) {
                    entry._onError = null;
                    this._settledAt(entry._state, i, entry._result, firstPass);
                } else if (typeof then$$1 !== 'function') {
                    this._settledAt(FULFILLED, i, entry, firstPass);
                } else if (this._isUsingOwnPromise) {
                    var promise = new c(noop);
                    handleMaybeThenable(promise, entry, then$$1);
                    this._willSettleAt(promise, i, firstPass);
                } else {
                    this._willSettleAt(new c(function (resolve) {
                        return resolve(entry);
                    }), i, firstPass);
                }
            } else {
                this._willSettleAt(c.resolve(entry), i, firstPass);
            }
        };
        Enumerator.prototype._eachEntry = function _eachEntry(entry, i, firstPass) {
            if (entry !== null && typeof entry === 'object') {
                this._settleMaybeThenable(entry, i, firstPass);
            } else {
                this._setResultAt(FULFILLED, i, entry, firstPass);
            }
        };
        Enumerator.prototype._settledAt = function _settledAt(state, i, value, firstPass) {
            var promise = this.promise;
            if (promise._state === PENDING) {
                if (this._abortOnReject && state === REJECTED) {
                    reject(promise, value);
                } else {
                    this._setResultAt(state, i, value, firstPass);
                    this._checkFullfillment();
                }
            }
        };
        Enumerator.prototype._setResultAt = function _setResultAt(state, i, value, firstPass) {
            this._remaining--;
            this._result[i] = value;
        };
        Enumerator.prototype._willSettleAt = function _willSettleAt(promise, i, firstPass) {
            var _this = this;
            subscribe(promise, undefined, function (value) {
                return _this._settledAt(FULFILLED, i, value, firstPass);
            }, function (reason) {
                return _this._settledAt(REJECTED, i, reason, firstPass);
            });
        };
        return Enumerator;
    }();
    function all(entries, label) {
        return new Enumerator(this, entries, true, label).promise;
    }
    function reject$1(reason, label) {
        var Constructor = this;
        var promise = new Constructor(noop, label);
        reject(promise, reason);
        return promise;
    }
    var guidKey = 'async_' + Date.now() + '-';
    var counter = 0;
    function needsResolver() {}
    function needsNew() {}
    var Promise = function () {
        function Promise(resolver, label) {
            this._id = counter++;
            this._label = label;
            this._state = undefined;
            this._result = undefined;
            this._subscribers = [];
            config.instrument && instrument('created', this);
            if (noop !== resolver) {
                typeof resolver !== 'function' && needsResolver();
                this instanceof Promise ? initializePromise(this, resolver) : needsNew();
            }
        }
        Promise.prototype._onError = function _onError(reason) {
            var _this = this;
            config.after(function () {
                if (_this._onError) {
                    config.trigger('error', reason, _this._label);
                }
            });
        };
        Promise.prototype.catch = function _catch(onRejection, label) {
            return this.then(undefined, onRejection, label);
        };
        return Promise;
    }();
    Promise.all = all;
    Promise.resolve = resolve$$1;
    Promise.reject = reject$1;
    Promise.prototype._guidKey = guidKey;
    Promise.prototype.then = then;
    function all$1(array, label) {
        return Promise.all(array, label);
    }
    function resolve$2(value, label) {
        return Promise.resolve(value, label);
    }
    function reject$2(reason, label) {
        return Promise.reject(reason, label);
    }
    var len = 0;
    function asap(callback, arg) {
        queue$1[len] = callback;
        queue$1[len + 1] = arg;
        len += 2;
        if (len === 2) {
            scheduleFlush$1();
        }
    }
    var browserWindow = typeof window !== 'undefined' ? window : undefined;
    var browserGlobal = browserWindow || {};
    var BrowserMutationObserver = browserGlobal.MutationObserver || browserGlobal.WebKitMutationObserver;
    var isNode = typeof self === 'undefined' && typeof process !== 'undefined' && {}.toString.call(process) === '[object process]';
    var isWorker = typeof Uint8ClampedArray !== 'undefined' && typeof importScripts !== 'undefined' && typeof MessageChannel !== 'undefined';
    function useNextTick() {
        var nextTick = process.nextTick;
        var version = process.versions.node.match(/^(?:(\d+)\.)?(?:(\d+)\.)?(\*|\d+)$/);
        if (Array.isArray(version) && version[1] === '0' && version[2] === '10') {
            nextTick = setImmediate;
        }
        return function () {
            return nextTick(flush);
        };
    }
    function useMutationObserver() {
        var iterations = 0;
        var observer = new BrowserMutationObserver(flush);
        var node = document.createTextNode('');
        observer.observe(node, { characterData: true });
        return function () {
            return node.data = iterations = ++iterations % 2;
        };
    }
    function useMessageChannel() {
        var channel = new MessageChannel();
        channel.port1.onmessage = flush;
        return function () {
            return channel.port2.postMessage(0);
        };
    }
    function useSetTimeout() {
        return function () {
            return setTimeout(flush, 1);
        };
    }
    var queue$1 = new Array(1000);
    function flush() {
        for (var i = 0; i < len; i += 2) {
            var callback = queue$1[i];
            var arg = queue$1[i + 1];
            callback(arg);
            queue$1[i] = undefined;
            queue$1[i + 1] = undefined;
        }
        len = 0;
    }
    var scheduleFlush$1 = void 0;
    if (isNode) {
        scheduleFlush$1 = useNextTick();
    } else if (BrowserMutationObserver) {
        scheduleFlush$1 = useMutationObserver();
    } else if (isWorker) {
        scheduleFlush$1 = useMessageChannel();
    } else {
        scheduleFlush$1 = useSetTimeout();
    }
    config.casync = asap;
    config.after = function (cb) {
        return setTimeout(cb, 0);
    };
    var casync = function (callback, arg) {
        return config.casync(callback, arg);
    };
    var async = {
        Promise: Promise,
        EventTarget: EventTarget,
        all: all$1,
        resolve: resolve$2,
        reject: reject$2,
    };
    exports.default = async;
    exports.Promise = Promise;
    exports.EventTarget = EventTarget;
    exports.all = all$1;
    exports.resolve = resolve$2;
    exports.reject = reject$2;
    exports.casync = casync;
    Object.defineProperty(exports, '__esModule', { value: true });
})));
var jscompress = (function() {
    var f = String.fromCharCode;
    var jscompress = {
        compress: function (uncompressed) {
            return jscompress._compress(uncompressed, 16, function(a){return f(a);});
        },
        _compress: function (uncompressed, bitsPerChar, getCharFromInt) {
            if (uncompressed == null) return "";
            var i, value,
                context_dictionary= {},
                context_dictionaryToCreate= {},
                context_c="",
                context_wc="",
                context_w="",
                context_enlargeIn= 2,
                context_dictSize= 3,
                context_numBits= 2,
                context_data=[],
                context_data_val=0,
                context_data_position=0,
                ii;
            for (ii = 0; ii < uncompressed.length; ii += 1) {
                context_c = uncompressed.charAt(ii);
                if (!Object.prototype.hasOwnProperty.call(context_dictionary,context_c)) {
                    context_dictionary[context_c] = context_dictSize++;
                    context_dictionaryToCreate[context_c] = true;
                }
                context_wc = context_w + context_c;
                if (Object.prototype.hasOwnProperty.call(context_dictionary,context_wc)) {
                    context_w = context_wc;
                } else {
                    if (Object.prototype.hasOwnProperty.call(context_dictionaryToCreate,context_w)) {
                        if (context_w.charCodeAt(0)<256) {
                            for (i=0 ; i<context_numBits ; i++) {
                                context_data_val = (context_data_val << 1);
                                if (context_data_position == bitsPerChar-1) {
                                    context_data_position = 0;
                                    context_data.push(getCharFromInt(context_data_val));
                                    context_data_val = 0;
                                } else {
                                    context_data_position++;
                                }
                            }
                            value = context_w.charCodeAt(0);
                            for (i=0 ; i<8 ; i++) {
                                context_data_val = (context_data_val << 1) | (value&1);
                                if (context_data_position == bitsPerChar-1) {
                                    context_data_position = 0;
                                    context_data.push(getCharFromInt(context_data_val));
                                    context_data_val = 0;
                                } else {
                                    context_data_position++;
                                }
                                value = value >> 1;
                            }
                        } else {
                            value = 1;
                            for (i=0 ; i<context_numBits ; i++) {
                                context_data_val = (context_data_val << 1) | value;
                                if (context_data_position ==bitsPerChar-1) {
                                    context_data_position = 0;
                                    context_data.push(getCharFromInt(context_data_val));
                                    context_data_val = 0;
                                } else {
                                    context_data_position++;
                                }
                                value = 0;
                            }
                            value = context_w.charCodeAt(0);
                            for (i=0 ; i<16 ; i++) {
                                context_data_val = (context_data_val << 1) | (value&1);
                                if (context_data_position == bitsPerChar-1) {
                                    context_data_position = 0;
                                    context_data.push(getCharFromInt(context_data_val));
                                    context_data_val = 0;
                                } else {
                                    context_data_position++;
                                }
                                value = value >> 1;
                            }
                        }
                        context_enlargeIn--;
                        if (context_enlargeIn == 0) {
                            context_enlargeIn = Math.pow(2, context_numBits);
                            context_numBits++;
                        }
                        delete context_dictionaryToCreate[context_w];
                    } else {
                        value = context_dictionary[context_w];
                        for (i=0 ; i<context_numBits ; i++) {
                            context_data_val = (context_data_val << 1) | (value&1);
                            if (context_data_position == bitsPerChar-1) {
                                context_data_position = 0;
                                context_data.push(getCharFromInt(context_data_val));
                                context_data_val = 0;
                            } else {
                                context_data_position++;
                            }
                            value = value >> 1;
                        }
                    }
                    context_enlargeIn--;
                    if (context_enlargeIn == 0) {
                        context_enlargeIn = Math.pow(2, context_numBits);
                        context_numBits++;
                    }
                    context_dictionary[context_wc] = context_dictSize++;
                    context_w = String(context_c);
                }
            }
            if (context_w !== "") {
                if (Object.prototype.hasOwnProperty.call(context_dictionaryToCreate,context_w)) {
                    if (context_w.charCodeAt(0)<256) {
                        for (i=0 ; i<context_numBits ; i++) {
                            context_data_val = (context_data_val << 1);
                            if (context_data_position == bitsPerChar-1) {
                                context_data_position = 0;
                                context_data.push(getCharFromInt(context_data_val));
                                context_data_val = 0;
                            } else {
                                context_data_position++;
                            }
                        }
                        value = context_w.charCodeAt(0);
                        for (i=0 ; i<8 ; i++) {
                            context_data_val = (context_data_val << 1) | (value&1);
                            if (context_data_position == bitsPerChar-1) {
                                context_data_position = 0;
                                context_data.push(getCharFromInt(context_data_val));
                                context_data_val = 0;
                            } else {
                                context_data_position++;
                            }
                            value = value >> 1;
                        }
                    } else {
                        value = 1;
                        for (i=0 ; i<context_numBits ; i++) {
                            context_data_val = (context_data_val << 1) | value;
                            if (context_data_position == bitsPerChar-1) {
                                context_data_position = 0;
                                context_data.push(getCharFromInt(context_data_val));
                                context_data_val = 0;
                            } else {
                                context_data_position++;
                            }
                            value = 0;
                        }
                        value = context_w.charCodeAt(0);
                        for (i=0 ; i<16 ; i++) {
                            context_data_val = (context_data_val << 1) | (value&1);
                            if (context_data_position == bitsPerChar-1) {
                                context_data_position = 0;
                                context_data.push(getCharFromInt(context_data_val));
                                context_data_val = 0;
                            } else {
                                context_data_position++;
                            }
                            value = value >> 1;
                        }
                    }
                    context_enlargeIn--;
                    if (context_enlargeIn == 0) {
                        context_enlargeIn = Math.pow(2, context_numBits);
                        context_numBits++;
                    }
                    delete context_dictionaryToCreate[context_w];
                } else {
                    value = context_dictionary[context_w];
                    for (i=0 ; i<context_numBits ; i++) {
                        context_data_val = (context_data_val << 1) | (value&1);
                        if (context_data_position == bitsPerChar-1) {
                            context_data_position = 0;
                            context_data.push(getCharFromInt(context_data_val));
                            context_data_val = 0;
                        } else {
                            context_data_position++;
                        }
                        value = value >> 1;
                    }
                }
                context_enlargeIn--;
                if (context_enlargeIn == 0) {
                    context_enlargeIn = Math.pow(2, context_numBits);
                    context_numBits++;
                }
            }
            value = 2;
            for (i=0 ; i<context_numBits ; i++) {
                context_data_val = (context_data_val << 1) | (value&1);
                if (context_data_position == bitsPerChar-1) {
                    context_data_position = 0;
                    context_data.push(getCharFromInt(context_data_val));
                    context_data_val = 0;
                } else {
                    context_data_position++;
                }
                value = value >> 1;
            }
            while (true) {
                context_data_val = (context_data_val << 1);
                if (context_data_position == bitsPerChar-1) {
                    context_data.push(getCharFromInt(context_data_val));
                    break;
                }
                else context_data_position++;
            }
            return context_data.join('');
        },
        decompress: function (compressed) {
            if (compressed == null) return "";
            if (compressed == "") return null;
            return jscompress._decompress(compressed.length, 32768, function(index) { return compressed.charCodeAt(index); });
        },
        _decompress: function (length, resetValue, getNextValue) {
            var dictionary = [],
                next,
                enlargeIn = 4,
                dictSize = 4,
                numBits = 3,
                entry = "",
                result = [],
                i,
                w,
                bits, resb, maxpower, power,
                c,
                data = {val:getNextValue(0), position:resetValue, index:1};
            for (i = 0; i < 3; i += 1) {
                dictionary[i] = i;
            }
            bits = 0;
            maxpower = Math.pow(2,2);
            power=1;
            while (power!=maxpower) {
                resb = data.val & data.position;
                data.position >>= 1;
                if (data.position == 0) {
                    data.position = resetValue;
                    data.val = getNextValue(data.index++);
                }
                bits |= (resb>0 ? 1 : 0) * power;
                power <<= 1;
            }
            switch (next = bits) {
                case 0:
                    bits = 0;
                    maxpower = Math.pow(2,8);
                    power=1;
                    while (power!=maxpower) {
                        resb = data.val & data.position;
                        data.position >>= 1;
                        if (data.position == 0) {
                            data.position = resetValue;
                            data.val = getNextValue(data.index++);
                        }
                        bits |= (resb>0 ? 1 : 0) * power;
                        power <<= 1;
                    }
                    c = f(bits);
                    break;
                case 1:
                    bits = 0;
                    maxpower = Math.pow(2,16);
                    power=1;
                    while (power!=maxpower) {
                        resb = data.val & data.position;
                        data.position >>= 1;
                        if (data.position == 0) {
                            data.position = resetValue;
                            data.val = getNextValue(data.index++);
                        }
                        bits |= (resb>0 ? 1 : 0) * power;
                        power <<= 1;
                    }
                    c = f(bits);
                    break;
                case 2:
                    return "";
            }
            dictionary[3] = c;
            w = c;
            result.push(c);
            while (true) {
                if (data.index > length) {
                    return "";
                }
                bits = 0;
                maxpower = Math.pow(2,numBits);
                power=1;
                while (power!=maxpower) {
                    resb = data.val & data.position;
                    data.position >>= 1;
                    if (data.position == 0) {
                        data.position = resetValue;
                        data.val = getNextValue(data.index++);
                    }
                    bits |= (resb>0 ? 1 : 0) * power;
                    power <<= 1;
                }
                switch (c = bits) {
                    case 0:
                        bits = 0;
                        maxpower = Math.pow(2,8);
                        power=1;
                        while (power!=maxpower) {
                            resb = data.val & data.position;
                            data.position >>= 1;
                            if (data.position == 0) {
                                data.position = resetValue;
                                data.val = getNextValue(data.index++);
                            }
                            bits |= (resb>0 ? 1 : 0) * power;
                            power <<= 1;
                        }
                        dictionary[dictSize++] = f(bits);
                        c = dictSize-1;
                        enlargeIn--;
                        break;
                    case 1:
                        bits = 0;
                        maxpower = Math.pow(2,16);
                        power=1;
                        while (power!=maxpower) {
                            resb = data.val & data.position;
                            data.position >>= 1;
                            if (data.position == 0) {
                                data.position = resetValue;
                                data.val = getNextValue(data.index++);
                            }
                            bits |= (resb>0 ? 1 : 0) * power;
                            power <<= 1;
                        }
                        dictionary[dictSize++] = f(bits);
                        c = dictSize-1;
                        enlargeIn--;
                        break;
                    case 2:
                        return result.join('');
                }
                if (enlargeIn == 0) {
                    enlargeIn = Math.pow(2, numBits);
                    numBits++;
                }
                if (dictionary[c]) {
                    entry = dictionary[c];
                } else {
                    if (c === dictSize) {
                        entry = w + w.charAt(0);
                    } else {
                        return null;
                    }
                }
                result.push(entry);
                dictionary[dictSize++] = w + entry.charAt(0);
                enlargeIn--;
                w = entry;
                if (enlargeIn == 0) {
                    enlargeIn = Math.pow(2, numBits);
                    numBits++;
                }
            }
        }
    };
    return jscompress;
})();
if (typeof define === 'function' && define.amd) {
    define(function () { return jscompress; });
} else if( typeof module !== 'undefined' && module != null ) {
    module.exports = jscompress
}
(function( window, document ) {
    'use strict';
    var head = document.head || document.getElementsByTagName('head')[0];
    var storagePrefix = 'load-';
    var defaultExpiration = 50000;
    var inLscache = [];
    var progressHandlers = null;
    var addLocalStorage = function( key, storeObj ) {
        try {
            localStorage.setItem( storagePrefix + key, JSON.stringify( storeObj ) );
            return true;
        } catch( e ) {
            if ( e.name.toUpperCase().indexOf('QUOTA') >= 0 ) {
                var item;
                var tempScripts = [];
                for ( item in localStorage ) {
                    if ( item.indexOf( storagePrefix ) === 0 ) {
                        tempScripts.push( JSON.parse( localStorage[ item ] ) );
                    }
                }
                if ( tempScripts.length ) {
                    tempScripts.sort(function( a, b ) {
                        return a.stamp - b.stamp;
                    });
                    lscache.remove( tempScripts[ 0 ].key );
                    return addLocalStorage( key, storeObj );
                } else {
                    return;
                }
            } else {
                return;
            }
        }
    }
    var checkStoreOnStorage = function( storeObj ) {
        var maxLength = Math.pow(2,24);
        var preLength = 0;
        var hugeString = "0";
        var testString;
        var keyName = "testingLengthKey";
        testString = (new Array(Math.pow(2, 24))).join("X");
        while (maxLength !== preLength) {
            try  {
                localStorage.setItem(keyName, testString);
                preLength = testString.length;
                maxLength = Math.ceil(preLength + ((hugeString.length - preLength) / 2));
                testString = hugeString.substr(0, maxLength);
            } catch (e) {
                hugeString = testString;
                maxLength = Math.floor(testString.length - (testString.length - preLength) / 2);
                testString = hugeString.substr(0, maxLength);
            }
        }
        localStorage.removeItem(keyName);
        maxLength = maxLength + keyName.length - 2;
        var storeObjDataLength = storeObj.data.length;
        if (storeObjDataLength < maxLength) {
            return true;
        } else {
            return false;
        }
    }
    var getUrl = function( url ) {
        var promise = new async.Promise( function( resolve, reject ){
            var xhr = new XMLHttpRequest();
            xhr.open( 'GET', url );
            xhr.addEventListener("progress", function(evt) {
                if (progressHandlers) {
                    progressHandlers(evt)
                }
            }, false);
            xhr.onreadystatechange = function() {
                if ( xhr.readyState === 4 ) {
                    if ( ( xhr.status === 200 ) ||
                        ( ( xhr.status === 0 ) && xhr.responseText ) ) {
                        resolve( {
                            content: xhr.responseText,
                            type: xhr.getResponseHeader('content-type')
                        } );
                    } else {
                        reject( new Error( xhr.statusText ) );
                    }
                }
            }
            setTimeout( function () {
                if( xhr.readyState < 4 ) {
                    xhr.abort();
                }
            }, lscache.timeout );

            xhr.send();
        });
        return promise;
    };
    var saveUrl = function( obj ) {
        return getUrl( obj.url ).then( function( result ) {
            var storeObj = wrapStoreData( obj, result );
            if (!obj.skipCache) {
                if (checkStoreOnStorage( storeObj )) {
                    addLocalStorage( obj.key , storeObj );
                }
            }
            return storeObj;
        });
    };
    var wrapStoreData = function( obj, data ) {
        var now = +new Date();
        obj.data = jscompress.compress(data.content);
        obj.originalType = data.type;
        obj.type = obj.type || data.type;
        obj.skipCache = obj.skipCache || false;
        obj.stamp = now;
        obj.expire = now + ( ( obj.expire || defaultExpiration ) * 60 * 60 * 1000 );
        return obj;
    };
    var isCacheValid = function(source, obj) {
        return !source ||
            source.expire - +new Date() < 0  ||
            obj.unique !== source.unique ||
            (lscache.isValidItem && !lscache.isValidItem(source, obj));
    };
    var handleStackObject = function( obj ) {
        var source, promise, shouldFetch;
        if ( !obj.url ) {
            return;
        }
        obj.key =  ( obj.key || obj.url );
        source = lscache.get( obj.key );
        obj.execute = obj.execute !== false;
        shouldFetch = isCacheValid(source, obj);
        if( obj.live || shouldFetch ) {
            if ( obj.unique ) {
                obj.url += ( ( obj.url.indexOf('?') > 0 ) ? '&' : '?' ) + 'lscache-unique=' + obj.unique;
            }
            if (obj.type != 'data') {
                promise = saveUrl( obj );
            }
            if( obj.live && !shouldFetch ) {
                promise = promise
                    .then( function( result ) {
                        return result;
                    }, function() {
                        return source;
                    });
            }
        } else {
            source.type = obj.type || source.originalType;
            source.execute = obj.execute;
            promise = new async.Promise( function( resolve ){
                resolve( source );
            });
        }
        return promise;
    };
    var injectScript = function( obj ) {
        var script = document.createElement('script');
        script.defer = true;
        script.text = jscompress.decompress(obj.data);
        head.appendChild( script );
    };
    var handlers = {
        'default': injectScript,
    };
    var execute = function( obj ) {
        if( obj.type && handlers[ obj.type ] ) {
            return handlers[ obj.type ]( obj );
        }
        return handlers['default']( obj );
    };
    var performActions = function( resources ) {
        return resources.map( function( obj ) {
            if (obj) {
                if( obj.execute ) {
                    execute( obj );
                }
            }
            return obj;
        } );
    };
    var fetch = function() {
        var i, l, promises = [];
        for ( i = 0, l = arguments.length; i < l; i++ ) {
            promises.push( handleStackObject( arguments[ i ] ) );
        }
        return async.all( promises );
    };
    var thenRequire = function() {
        var resources = fetch.apply( null, arguments );
        var promise = this.then( function() {
            return resources;
        }).then( performActions );
        promise.thenRequire = thenRequire;
        return promise;
    };
    window.lscache = {
        require: function() {
            for ( var a = 0, l = arguments.length; a < l; a++ ) {
                arguments[a].execute = arguments[a].execute !== false;
                if ( arguments[a].once && inLscache.indexOf(arguments[a].url) >= 0 ) {
                    arguments[a].execute = false;
                } else if ( arguments[a].execute !== false && inLscache.indexOf(arguments[a].url) < 0 ) {
                    inLscache.push(arguments[a].url);
                }
            }
            var promise = fetch.apply( null, arguments ).then( performActions );
            promise.thenRequire = thenRequire;
            return promise;
        },
        remove: function( key ) {
            localStorage.removeItem( storagePrefix + key );
            return this;
        },
        get: function( key ) {
            var item = localStorage.getItem( storagePrefix + key );
            try	{
                return JSON.parse( item || 'false' );
            } catch( e ) {
                return false;
            }
        },
        clear: function( expired ) {
            var item, key;
            var now = +new Date();
            for ( item in localStorage ) {
                key = item.split( storagePrefix )[ 1 ];
                if ( key && ( !expired || this.get( key ).expire <= now ) ) {
                    this.remove( key );
                }
            }
            return this;
        },
        isValidItem: null,
        timeout: 10000,
        addProgressHandler: function( handler ) {
            progressHandlers = handler;
            return this;
        },
    };
    lscache.clear( true );
})( this, document );
var memory_loaded = false;
var default_display_style_desktop = 2;
var default_display_style_tablet = 2;
var default_display_style_mobile = 2;
function bodyOnload() {
  memory_loaded = true;
}
$(function() {
  var emu;
  var ui;
  var link;
  //<![CDATA[
    function reset() {
      d0 = 0;
      d1 = 0;
      d2 = 0;
      d3 = 0;
      d4 = 0;
      d5 = 0;
      d6 = 0;
      d7 = 0;
      a0 = 0;
      a1 = 0;
      a2 = 0;
      a3 = 0;
      a4 = 0;
      a5 = 0;
      a6 = 0;
      a8 = 0x4C00;
      a7 = 0x4C00;
      ram = new Uint16Array(131072);
  };
  //]]>

  // ===================== Startup key macro =====================
  // After the OS has booted, automatically press a sequence of TI-89 keys.
  // Keys are given by their key-matrix number(s); listing several numbers in
  // one step presses them together (e.g. alpha+letter). Everything below is
  // meant to be tweaked freely.
  var startupMacroEnabled = true;      // set to false to turn the macro off
  var startupMacroInitialDelay = 6000; // ms to wait after the emulator starts (OS boot time)
  var startupMacroHold = 80;           // ms each key is held down before release

  // Each step: { keys: [matrix numbers], gap: ms to wait before the NEXT key }.
  // (The keyboard is emulated the same way the physical PC keyboard is: letters
  //  are alpha (7) + the letter's matrix key pressed simultaneously.)
  var startupMacroSequence = [
    { keys: [4],     gap: 200 }, // Alt  -> 2nd  [Varlink, Collapse All]
    { keys: [10],    gap: 500 }, // -
    { keys: [15],    gap: 200 }, // F5
    { keys: [26],    gap: 200 }, // 5
    { keys: [48],    gap: 500 }, // Esc

    { keys: [45],    gap: 100 }, // x    [x\inst()]
    { keys: [4],     gap: 100 }, // Alt -> 2nd
    { keys: [25],    gap: 100 }, // backslash
    { keys: [7, 19], gap: 100 }, // i  (alpha + key 19)
    { keys: [7, 18], gap: 100 }, // n  (alpha + key 18)
    { keys: [7, 17], gap: 100 }, // s  (alpha + key 17)
    { keys: [21],    gap: 100 }, // t
    { keys: [36],    gap: 100 }, // (
    { keys: [28],    gap: 100 }, // )
    { keys: [8],     gap: 3000 }, // ENTER

    { keys: [22],    gap: 100 }, // backspace (<-)   [clear last 2 lines]
    { keys: [0],     gap: 100 }, // UP (kurzor nahoru)
    { keys: [22],    gap: 100 },  // backspace (<-)

	{ keys: [6],     gap: 200 },  // diamond   [EQW shortcuds]
	{ keys: [40],    gap: 200 },  // APPS
	{ keys: [8],    gap: 200 },  // ENTER
	{ keys: [8],    gap: 500 },  // ENTER
	{ keys: [47],   gap: 200 },  // F1
	{ keys: [0],    gap: 200 },  // UP (kurzor nahoru)
	{ keys: [0],    gap: 200 },  // UP (kurzor nahoru)
	{ keys: [8],    gap: 500 },  // ENTER
	{ keys: [2],    gap: 200 },  // DOWN (kurzor dolu)
	{ keys: [3],    gap: 200 },  // RIGHT (kurzor doprava)
	{ keys: [0],    gap: 200 },  // UP (kurzor nahoru)
	{ keys: [8],    gap: 200 },  // ENTER
	{ keys: [8],    gap: 200 },  // ENTER
	{ keys: [46],   gap: 200 }  // HOME
  ];

  function pressMacroKey(keys) {
    if (typeof emu === "undefined" || !emu) { return; }
    var i;
    for (i = 0; i < keys.length; i++) { emu.setKey(keys[i], 1); }
    setTimeout(function () {
      for (i = 0; i < keys.length; i++) { emu.setKey(keys[i], 0); }
    }, startupMacroHold);
  }

  function runStartupMacro() {
    if (!startupMacroEnabled) { return; }
    var idx = 0;
    (function step() {
      if (idx >= startupMacroSequence.length) { return; }
      var s = startupMacroSequence[idx++];
      pressMacroKey(s.keys);
      setTimeout(step, s.gap);
    })();
  }
  // =================== End startup key macro ===================

  // ============================================================
  //  Emulator state persistence (IndexedDB)
  //  Saves the full machine snapshot (RAM, flash, CPU registers and
  //  hardware ports via emu.save_state()) so the calculator resumes where
  //  it left off after a reload / reopening the home-screen web app.
  //  Escape hatch: open with ?fresh in the URL to skip + clear the save.
  // ============================================================
  var __persistDB = 'ti89emu', __persistStore = 'state', __persistKey = 'autosave';
  var __snapshotVersion = 1;
  var __stateRestored = false;
  var __saving = false;
  var __autoSaveInstalled = false;

  var __romSigCached = null;
  function __romSignature() {
    // Cached once (see loadSimulator) from the pristine ROM, BEFORE the emulator
    // converts it to its live flash buffer and starts writing to it (TI-89 writes
    // to flash during use). Computing it live would give a different value each
    // session and break restore.
    if (__romSigCached !== null) { return __romSigCached; }
    if (typeof rom === 'undefined' || !rom || !rom.length) { return 'norom'; } // don't cache until rom exists
    var n = rom.length, h = n >>> 0, step = Math.max(1, (n / 512) | 0);
    for (var i = 0; i < n; i += step) { h = (Math.imul(h, 31) + rom[i]) >>> 0; }
    __romSigCached = n + ':' + h;
    return __romSigCached;
  }

  function __openDB() {
    return new Promise(function (resolve, reject) {
      if (!window.indexedDB) { reject(new Error('no indexedDB')); return; }
      var req = indexedDB.open(__persistDB, 1);
      req.onupgradeneeded = function () { req.result.createObjectStore(__persistStore); };
      req.onsuccess = function () { resolve(req.result); };
      req.onerror = function () { reject(req.error); };
    });
  }
  function __idbGet(key) {
    return __openDB().then(function (db) {
      return new Promise(function (resolve, reject) {
        var r = db.transaction(__persistStore, 'readonly').objectStore(__persistStore).get(key);
        r.onsuccess = function () { resolve(r.result); };
        r.onerror = function () { reject(r.error); };
      });
    });
  }
  function __idbPut(key, val) {
    return __openDB().then(function (db) {
      return new Promise(function (resolve, reject) {
        var tx = db.transaction(__persistStore, 'readwrite');
        tx.objectStore(__persistStore).put(val, key);
        tx.oncomplete = function () { resolve(true); };
        tx.onerror = function () { reject(tx.error); };
      });
    });
  }
  function __idbDelete(key) {
    return __openDB().then(function (db) {
      return new Promise(function (resolve) {
        var tx = db.transaction(__persistStore, 'readwrite');
        tx.objectStore(__persistStore).delete(key);
        tx.oncomplete = function () { resolve(true); };
        tx.onerror = function () { resolve(false); };
      });
    }).catch(function () { return false; });
  }

  var __persistDebug = false; // logs to console; turn on with ti89state.debug(true)
  function __plog() {
    if (!__persistDebug || typeof console === 'undefined') { return; }
    var a = ['[ti89-persist]']; for (var i = 0; i < arguments.length; i++) { a.push(arguments[i]); }
    try { console.log.apply(console, a); } catch (e) {}
  }

  function saveEmulatorState() {
    if (typeof emu === 'undefined' || !emu || !emu.save_state) { __plog('save skipped: emu not ready'); return Promise.resolve(false); }
    if (__saving) { return Promise.resolve(false); }
    __saving = true;
    try {
      var sig = __romSignature();
      var rec = { v: __snapshotVersion, rom: sig, ts: Date.now(), snap: emu.save_state() };
      return __idbPut(__persistKey, rec)
        .then(function () { __saving = false; __plog('saved ok (romSig=' + sig + ')'); return true; })
        .catch(function (e) { __saving = false; __plog('save FAILED (idb put):', e && e.message); return false; });
    } catch (e) { __saving = false; __plog('save FAILED (save_state threw):', e && e.message); return Promise.resolve(false); }
  }

  function __tryRestoreState() {
    return __idbGet(__persistKey).then(function (rec) {
      if (!rec) { __plog('restore: no saved state found'); return false; }
      if (rec.v !== __snapshotVersion) { __plog('restore: version mismatch', rec.v, '!=', __snapshotVersion); return false; }
      var sig = __romSignature();
      if (rec.rom !== sig) { __plog('restore: ROM signature mismatch (saved ' + rec.rom + ' vs now ' + sig + ') -> booting fresh'); return false; }
      if (!emu || !emu.restore_state) { __plog('restore: emu.restore_state unavailable'); return false; }
      var ok = !!emu.restore_state(rec.snap);
      __plog('restore_state returned', ok, '(saved at ' + new Date(rec.ts).toLocaleString() + ')');
      return ok;
    }).catch(function (e) { __plog('restore FAILED:', e && e.message); return false; });
  }

  function __installAutoSave() {
    if (__autoSaveInstalled) { return; }
    __autoSaveInstalled = true;
    // Backgrounding/closing is the critical moment on iOS: save then.
    document.addEventListener('visibilitychange', function () {
      if (document.visibilityState === 'hidden') { saveEmulatorState(); }
    });
    window.addEventListener('pagehide', function () { saveEmulatorState(); });
    // A baseline save shortly after boot (so a reload always finds something),
    // then a periodic safety-net save. The pagehide/visibility saves above are
    // best-effort (async IndexedDB may not finish at unload), so these matter.
    setTimeout(saveEmulatorState, 2000);
    setInterval(saveEmulatorState, 8000);
    __plog('auto-save installed (baseline 2s, interval 8s)');
  }

  // Called from loadSimulator() right after emu.initemu().
  // onDecided(fresh): fresh === true when no state was restored (fresh boot).
  function initPersistence(onDecided) {
    var fresh = function (v) { __plog(v ? 'decision: FRESH boot (run macro)' : 'decision: RESTORED (skip macro)'); try { onDecided(v); } catch (e) {} };
    var params;
    try { params = new URLSearchParams(location.search); } catch (e) { params = null; }
    if (params && (params.has('fresh') || params.has('nosave'))) {
      __plog('?fresh/?nosave -> clearing saved state, fresh boot');
      __idbDelete(__persistKey);
      __installAutoSave();
      fresh(true);
      return;
    }
    if (navigator.storage && navigator.storage.persist) {
      try { navigator.storage.persist().then(function (p) { __plog('storage.persist() granted =', p); }); } catch (e) {}
    }

    __tryRestoreState().then(function (ok) {
      __stateRestored = ok;
      __installAutoSave();
      fresh(!ok);
    });
  }

  // ---- brief on-screen toast ----
  function __toast(msg) {
    var t = document.createElement('div');
    t.textContent = msg;
    t.style.cssText = 'position:fixed;left:50%;bottom:24px;transform:translateX(-50%);background:rgba(0,0,0,.82);color:#fff;padding:9px 16px;border-radius:16px;font:13px system-ui,-apple-system,sans-serif;z-index:1001;pointer-events:none;';
    document.body.appendChild(t);
    setTimeout(function () { if (t.parentNode) t.parentNode.removeChild(t); }, 1600);
  }

  // ---- base64 <-> ArrayBuffer (for file export/import) ----
  function __b64FromBuf(buf) {
    var bytes = new Uint8Array(buf), CHUNK = 0x8000, parts = [];
    for (var i = 0; i < bytes.length; i += CHUNK) {
      parts.push(String.fromCharCode.apply(null, bytes.subarray(i, i + CHUNK)));
    }
    return btoa(parts.join(''));
  }
  function __bufFromB64(b64) {
    var bin = atob(b64), len = bin.length, bytes = new Uint8Array(len);
    for (var i = 0; i < len; i++) { bytes[i] = bin.charCodeAt(i); }
    return bytes.buffer;
  }
  // convert a snapshot record to/from a JSON-safe form (ArrayBuffers -> base64)
  function __serializeRec(rec) {
    var out = { v: rec.v, rom: rec.rom, ts: rec.ts,
                snap: { apiversion: rec.snap.apiversion, link: rec.snap.link, ui: rec.snap.ui, flash: rec.snap.flash, emu: {} } };
    var e = rec.snap.emu;
    for (var k in e) {
      if (!e.hasOwnProperty(k)) continue;
      var val = e[k];
      if (val && typeof val === 'object' && val.__ta && val.buf) { out.snap.emu[k] = { __ta: val.__ta, b64: __b64FromBuf(val.buf) }; }
      else { out.snap.emu[k] = val; }
    }
    return out;
  }
  function __deserializeRec(obj) {
    var rec = { v: obj.v, rom: obj.rom, ts: obj.ts,
                snap: { apiversion: obj.snap.apiversion, link: obj.snap.link || {}, ui: obj.snap.ui || {}, flash: obj.snap.flash, emu: {} } };
    var e = obj.snap.emu;
    for (var k in e) {
      if (!e.hasOwnProperty(k)) continue;
      var val = e[k];
      if (val && typeof val === 'object' && val.__ta && val.b64) { rec.snap.emu[k] = { __ta: val.__ta, buf: __bufFromB64(val.b64) }; }
      else { rec.snap.emu[k] = val; }
    }
    return rec;
  }
  function __tsStr() {
    var d = new Date(), p = function (n) { return (n < 10 ? '0' : '') + n; };
    return d.getFullYear() + p(d.getMonth() + 1) + p(d.getDate()) + '-' + p(d.getHours()) + p(d.getMinutes()) + p(d.getSeconds());
  }

  function __downloadBlob(blob, fname) {
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob); a.download = fname;
    document.body.appendChild(a); a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); if (a.parentNode) a.parentNode.removeChild(a); }, 2000);
  }
  function __isIOS() {
    return /iP(hone|od|ad)/.test(navigator.platform) ||
           /iPad|iPhone|iPod/.test(navigator.userAgent) ||
           (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1); // iPadOS reports as Mac
  }
  function exportState() {
    if (typeof emu === 'undefined' || !emu || !emu.save_state) { __toast('Emulator not ready'); return; }
    var rec = { v: __snapshotVersion, rom: __romSignature(), ts: Date.now(), snap: emu.save_state() };
    var json = JSON.stringify(__serializeRec(rec));
    var fname = 'ti89-state-' + __tsStr() + '.json';
    var blob = new Blob([json], { type: 'application/json' });
    // On iOS the <a download> trick opens the file instead of saving it, so the
    // share sheet ("Save to Files") is the reliable path there. On desktop we
    // download directly -- desktop browsers may advertise the Web Share API but
    // then do nothing useful, which is why Export appeared to be a no-op on PC.
    if (__isIOS() && navigator.share && navigator.canShare) {
      var file = null;
      try { file = new File([blob], fname, { type: 'application/json' }); } catch (e) {}
      if (file && navigator.canShare({ files: [file] })) {
        navigator.share({ files: [file] }).catch(function () {});
        return;
      }
    }
    __downloadBlob(blob, fname);
  }

  function importStateFile(file) {
    var reader = new FileReader();
    reader.onload = function () {
      try {
        var obj = JSON.parse(reader.result);
        if (!obj || !obj.snap || !obj.snap.emu) { __toast('Invalid file'); return; }
        var rec = __deserializeRec(obj);
        if (rec.rom !== __romSignature() &&
            !window.confirm('This file is from a different ROM version than the one currently loaded. Load anyway? It may cause errors.')) { return; }
        if (emu && emu.restore_state && emu.restore_state(rec.snap)) {
          __idbPut(__persistKey, rec); // also persist so it survives reload
          __toast('State loaded');
        } else { __toast('Load failed'); }
      } catch (e) { __toast('Invalid file'); }
    };
    reader.readAsText(file);
  }
  function importStatePrompt() {
    var input = document.createElement('input');
    input.type = 'file'; input.accept = 'application/json,.json'; input.style.display = 'none';
    input.addEventListener('change', function () {
      if (input.files && input.files[0]) { importStateFile(input.files[0]); }
      if (input.parentNode) input.parentNode.removeChild(input);
    });
    document.body.appendChild(input);
    input.click();
  }

  // ---- load a binary file (ROM / OS upgrade / variable / FlashApp) ----
  // emu.loadrom() takes a browser File object directly and auto-detects the
  // type from the extension and size (plain .rom, .tib/.89u OS upgrades, and
  // the .89x/.89z/.89p/... single-variable and .89k FlashApp formats).
  function loadBinaryFile(file) {
    if (typeof emu === 'undefined' || !emu || !emu.loadrom) { __toast('Emulator not ready'); return; }
    try {
      emu.loadrom(file);
      __toast('Loading ' + file.name + '…');
    } catch (e) { __toast('Load failed'); }
  }
  function loadBinaryPrompt() {
    var input = document.createElement('input');
    input.type = 'file';
    // Extensions loadrom() recognises (89 / 92+ / V200 families + .rom/.tib).
    input.accept = [
      '.rom', '.tib',
      '.9xu', '.89u', '.v2u', '.9xa', '.89a', '.v2a', '.9xc', '.89c', '.v2c',
      '.9xd', '.89d', '.v2d', '.9xe', '.89e', '.v2e', '.9xf', '.89f', '.v2f',
      '.9xi', '.89i', '.v2i', '.9xk', '.89k', '.v2k', '.9xl', '.89l', '.v2l',
      '.9xm', '.89m', '.v2m', '.9xp', '.89p', '.v2p', '.9xs', '.89s', '.v2s',
      '.9xt', '.89t', '.v2t', '.9xx', '.89x', '.v2x', '.9xy', '.89y', '.v2y',
      '.9xz', '.89z', '.v2z'
    ].join(',');
    input.style.display = 'none';
    input.addEventListener('change', function () {
      if (input.files && input.files[0]) { loadBinaryFile(input.files[0]); }
      if (input.parentNode) input.parentNode.removeChild(input);
    });
    document.body.appendChild(input);
    input.click();
  }

  function resetState() {
    if (!window.confirm('Delete the saved state and start the calculator from scratch?')) { return; }
    __idbDelete(__persistKey).then(function () { location.reload(); });
  }

  // ---- right-click / long-press menu on the display ----
  function attachStateMenu() {
    var disp = document.getElementById('ti89-display');
    if (!disp || document.getElementById('ti89-menu')) { return; }

    var style = document.createElement('style');
    style.textContent =
      '#ti89-display, #ti89-display * { -webkit-touch-callout:none; -webkit-user-select:none; user-select:none; }' +
      '#ti89-menu { position:fixed; z-index:1000; display:none; min-width:190px; background:#1b211c; color:#e8ecdf;' +
      ' border:1px solid #3a4436; border-radius:8px; box-shadow:0 8px 28px rgba(0,0,0,.55); padding:6px;' +
      ' font:14px system-ui,-apple-system,sans-serif; -webkit-user-select:none; user-select:none; }' +
      '#ti89-menu .item { padding:11px 13px; border-radius:6px; cursor:pointer; white-space:nowrap; }' +
      '#ti89-menu .item:hover { background:#2c3527; }' +
      '#ti89-menu .sep { height:1px; background:#3a4436; margin:4px 6px; }';
    document.head.appendChild(style);

    var menu = document.createElement('div');
    menu.id = 'ti89-menu';
    var items = [
      ['Save state', function () { saveEmulatorState().then(function (ok) { __toast(ok ? 'Saved' : 'Save failed'); }); }],
      ['Export state…', exportState],
      ['Import state…', importStatePrompt],
      'sep',
      ['Load binary file…', loadBinaryPrompt],
      'sep',
      ['Reset (clear state)', resetState]
    ];
    items.forEach(function (it) {
      if (it === 'sep') { var s = document.createElement('div'); s.className = 'sep'; menu.appendChild(s); return; }
      var b = document.createElement('div');
      b.className = 'item'; b.textContent = it[0];
      var act = function (e) { e.preventDefault(); e.stopPropagation(); hideMenu(); it[1](); };
      b.addEventListener('click', act);
      menu.appendChild(b);
    });
    document.body.appendChild(menu);

    function showMenu(x, y) {
      menu.style.display = 'block';
      var w = menu.offsetWidth, h = menu.offsetHeight;
      if (x + w > window.innerWidth) { x = window.innerWidth - w - 8; }
      if (y + h > window.innerHeight) { y = window.innerHeight - h - 8; }
      menu.style.left = Math.max(4, x) + 'px';
      menu.style.top = Math.max(4, y) + 'px';
    }
    function hideMenu() { menu.style.display = 'none'; }

    disp.addEventListener('contextmenu', function (e) { e.preventDefault(); showMenu(e.clientX, e.clientY); });

    var lpTimer = null, sx = 0, sy = 0;
    disp.addEventListener('touchstart', function (e) {
      if (e.touches.length !== 1) { return; }
      sx = e.touches[0].clientX; sy = e.touches[0].clientY;
      lpTimer = setTimeout(function () { lpTimer = null; showMenu(sx, sy); }, 500);
    }, { passive: true });
    disp.addEventListener('touchmove', function (e) {
      if (!lpTimer) { return; }
      var t = e.touches[0];
      if (Math.abs(t.clientX - sx) > 10 || Math.abs(t.clientY - sy) > 10) { clearTimeout(lpTimer); lpTimer = null; }
    }, { passive: true });
    function cancelLP() { if (lpTimer) { clearTimeout(lpTimer); lpTimer = null; } }
    disp.addEventListener('touchend', cancelLP);
    disp.addEventListener('touchcancel', cancelLP);

    document.addEventListener('mousedown', function (e) { if (menu.style.display === 'block' && !menu.contains(e.target)) { hideMenu(); } });
    document.addEventListener('touchstart', function (e) { if (menu.style.display === 'block' && !menu.contains(e.target)) { hideMenu(); } }, { passive: true });
  }
  window.__ti89attachStateMenu = attachStateMenu;

  // Manual helpers (console / bookmarklet):
  //   ti89state.save() / .clear() / .info() / .export() / .import() / .reset() / .debug(b)
  window.ti89state = {
    save: saveEmulatorState,
    clear: function () { return __idbDelete(__persistKey); },
    export: exportState,
    import: importStatePrompt,
    loadBinary: loadBinaryPrompt,
    reset: resetState,
    info: function () {
      return __idbGet(__persistKey).then(function (rec) {
        if (!rec) { __plog('info: no saved state'); return null; }
        __plog('info: saved', new Date(rec.ts).toLocaleString(), 'v=' + rec.v, 'romSig=' + rec.rom, 'currentRomSig=' + __romSignature());
        return { ts: rec.ts, v: rec.v, rom: rec.rom, currentRom: __romSignature() };
      });
    },
    debug: function (b) { __persistDebug = !!b; return __persistDebug; }
  };

  function loadSimulator() {
    __romSignature(); // fingerprint the pristine ROM before the emulator converts/mutates it
    emu = TI68kEmulatorCoreModule(window);
    ui = TI68kEmulatorUIModule(window);
    link = TI68kEmulatorLinkModule(window);
    if (typeof(rom) === "object") {
      emu.setRom(rom);
    }
    emu.setReset(reset);
    ui.setEmu(emu);
    ui.setLink(link);
    emu.setUI(ui);
    emu.setLink(link);
    link.setEmu(emu);
    link.setUI(ui);
    emu.initemu();
    // Auto re-align the display frame, darkened mask and F1-F5 keys once the
    // emulator has initialised and painted its first frame. This replaces the
    // old manual "wiggle the adjust bar" workaround: on slower/uncached loads
    // the emulator used to become ready after the last layout pass, leaving the
    // mask and F-keys sized for an earlier state.
    adjustComponentsSize();
    requestAnimationFrame(adjustComponentsSize);
    setTimeout(adjustComponentsSize, 300);
    setTimeout(adjustComponentsSize, 1200);
    $('#calccontainer #calcback').css('display', "block");
    $('#calccontainer #calcsceen').css('display', "block");

    // Restore a previously saved machine state if one exists (and matches this
    // ROM); otherwise treat it as a fresh boot and run the startup key macro.
    initPersistence(function (freshBoot) {
      if (freshBoot && startupMacroEnabled) {
        setTimeout(runStartupMacro, startupMacroInitialDelay);
      }
    });
  }
  function downloadV12MV12() {
    // If the ROM and emulator are already present (loaded via <script> tags in
    // index.html), start immediately. This avoids the XMLHttpRequest fetch,
    // which browsers block for local file:// URLs, so the page can run just by
    // opening index.html. The XHR path below stays as a fallback for setups
    // that don't include those script tags (served over http:// / localhost).
    if (typeof rom !== 'undefined' && typeof TI68kEmulatorCoreModule === 'function') {
      loadSimulator();
      return;
    }
    lscache.require({ url: 'rom/ti89rom.js' }).then(function () {
      lscache.require({ url: 'js/v12.js' }).then(function () {
        loadSimulator();
      });
    });
  }
  var displayStyle = 2;
//   const calcContainerWidth1 = 10480; const calcContainerLeft1 = 1250; const calcContainerRight1 = 1330;
//   const calcContainerHeight1 = 8400; const calcContainerTop1 = 460; const calcContainerBottom1 = 3310;
//   const f1Top1 = 8725; const f1Left1 = 1157; const f2Top1 = 8837; const f2Left1 = 2588; const f3Top1 = 8905; const f3Left1 = 4050;
//   const f4Top1 = 8837; const f4Left1 = 5560; const f5Top1 = 8735; const f5Left1 = 7032;
//   var pressedFnHeight1 = 867; var pressedFnWidth1 = 867;

//   const calcContainerWidth2 = 16277; const calcContainerLeft2 = 2890; const calcContainerRight2 = 1230;
//   const calcContainerHeight2 = 11643; const calcContainerTop2 = 890; const calcContainerBottom2 = 830;
//   const f1Top2 = 1000; const f1Left2 = 920; const f2Top2 = 2975; const f2Left2 = 920; const f3Top2 = 4940; const f3Left2 = 920;
//   const f4Top2 = 6915; const f4Left2 = 920; const f5Top2 = 8910; const f5Left2 = 920;
//   var pressedFnHeight2 = 1120; var pressedFnWidth2 = 1120;


  var imageWidth;
  var imageHeight;
  var imageTop;
  var imageLeft;
  var keysContainerHeight;
  var keysWidth;
  var keysHeight;
  var keysTop;
  var keysLeft;
  var keysMarginTop = 5;
  var keysMarginBottom = 5;
  var keysMarginLeft = 5;
  var keysMarginRight = 5;
  var windowWidth = window.innerWidth;
  var windowHeight = window.innerHeight;
  var divideTopRatio = 0.35;
  // Fixed layout: user resizing of the display/keyboard ratio is disabled.
  // The ratio is computed so the display frame is exactly as wide as the keyboard.
  var userResizeEnabled = false;
  const minSkinHeight = 100;
  const minKeyboardHeight = 100;
  const adjustHeight = 25;
  var disclaimerExpireFlag = false;
  var disclaimerTooltipVisible = false;
  function setDefaultDisplay() {
    var windowWidth = window.innerWidth;
      displayStyle = 2;
    //   if (displayStyle == 2) {
    //     divideTopRatio = windowWidth * calcContainerHeight2 / calcContainerWidth2 / windowHeight;
    //   }
  }
  setDefaultDisplay();
  function adjustComponentsSize() {
    if (window.__ti89skin && window.__ti89skin.ready) { window.__ti89skin.layout(); return; }
  }
  // ==============================================================
  //  Classic TI-89 HW1 skin: keyboard image + transparent hit areas.
  //  Replaces the old Titanium device image and button-grid layout.
  //  Display (LCD) and keyboard are two separate, independently sized
  //  regions, scaled together to fill the screen (mobile-first).
  // ==============================================================
  (function initHw1Skin() {
    var KB_W = 389, KB_H = 581;            // keyboard.png intrinsic size (px)
    var DISP_ASPECT = 1.6;                 // LCD 160x100 -> square pixels
    var SPAN_L = 0.0386, SPAN_R = 0.9820;  // display width = key span (kb fractions)
    var GAP = 0.02;                        // gap between display and keyboard (fraction of kb width)
    // [name, setKey(number)|"ON", x0,y0,x1,y1]  (fractions of the keyboard image)
    var KEYS = [
      ["F1",47,0.0386,0.0086,0.1877,0.0826],["F2",39,0.2288,0.0086,0.3779,0.0826],
      ["F3",31,0.4165,0.0086,0.5656,0.0826],["F4",23,0.6067,0.0086,0.7558,0.0826],
      ["F5",15,0.7969,0.0086,0.9460,0.0826],
      ["2nd",4,0.0386,0.1583,0.1877,0.2324],["UP",5,0.2288,0.1583,0.3779,0.2324],
      ["ESC",48,0.4165,0.1583,0.5656,0.2324],
      ["cur_up",0,0.7069,0.1119,0.8638,0.1859],["cur_left",1,0.5913,0.1704,0.7018,0.2754],
      ["cur_right",3,0.8715,0.1704,0.9820,0.2754],["cur_down",2,0.7069,0.2599,0.8638,0.3339],
      ["diamond",6,0.0386,0.2478,0.1877,0.3219],["alpha",7,0.2288,0.2478,0.3779,0.3219],
      ["APPS",40,0.4165,0.2478,0.5656,0.3219],
      ["HOME",46,0.0386,0.3477,0.1877,0.4217],["MODE",38,0.2288,0.3477,0.3779,0.4217],
      ["CATALOG",30,0.4165,0.3477,0.5656,0.4217],["BACK",22,0.6067,0.3477,0.7558,0.4217],
      ["CLEAR",14,0.7969,0.3477,0.9460,0.4217],
      ["X",45,0.0386,0.4406,0.1877,0.5146],["Y",37,0.2288,0.4406,0.3779,0.5146],
      ["Z",29,0.4165,0.4406,0.5656,0.5146],["T",21,0.6067,0.4406,0.7558,0.5146],
      ["pow",13,0.7969,0.4406,0.9460,0.5146],
      ["equal",44,0.0386,0.5336,0.1877,0.6076],["lparen",36,0.2288,0.5336,0.3779,0.6076],
      ["rparen",28,0.4165,0.5336,0.5656,0.6076],["comma",20,0.6067,0.5336,0.7558,0.6076],
      ["divide",12,0.7969,0.5336,0.9460,0.6076],
      ["pipe",43,0.0386,0.6265,0.1877,0.7005],["7",35,0.2288,0.6265,0.3779,0.7005],
      ["8",27,0.4165,0.6265,0.5656,0.7005],["9",19,0.6067,0.6265,0.7558,0.7005],
      ["mult",11,0.7969,0.6265,0.9460,0.7005],
      ["EE",42,0.0386,0.7229,0.1877,0.7969],["4",34,0.2288,0.7229,0.3779,0.7969],
      ["5",26,0.4165,0.7229,0.5656,0.7969],["6",18,0.6067,0.7229,0.7558,0.7969],
      ["minus",10,0.7969,0.7229,0.9460,0.7969],
      ["STO",41,0.0386,0.8124,0.1877,0.8864],["1",33,0.2288,0.8124,0.3779,0.8864],
      ["2",25,0.4165,0.8124,0.5656,0.8864],["3",17,0.6067,0.8124,0.7558,0.8864],
      ["plus",9,0.7969,0.8124,0.9460,0.8864],
      ["ON","ON",0.0386,0.9053,0.1877,0.9793],["0",32,0.2288,0.9053,0.3779,0.9793],
      ["dot",24,0.4165,0.9053,0.5656,0.9793],["negate",16,0.6067,0.9053,0.7558,0.9793],
      ["ENTER",8,0.7969,0.9053,0.9460,0.9793]
    ];

    var css = document.createElement('style');
    css.textContent =
      'html,body{margin:0;padding:0;overflow:hidden;overscroll-behavior:none;}' +
      '#skinroot{position:fixed;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;background:#0e1512;overflow:hidden;z-index:10;touch-action:none;}' +
      '#ti89-display{position:relative;background:#0a0d0b;border:1px solid #000;box-sizing:border-box;overflow:hidden;}' +
      '#ti89-display #screen{display:block !important;position:absolute !important;top:0 !important;left:0 !important;width:100% !important;height:100% !important;image-rendering:pixelated;image-rendering:crisp-edges;background:#c7e6bf;}' +
      '#ti89-display #ti89-tint{position:absolute;inset:0;z-index:1;pointer-events:none;background:rgba(118,124,88,0.65);}' +
      '#ti89-keyboard{position:relative;}' +
      '#ti89-keyboard img{display:block;width:100%;height:100%;pointer-events:none;user-select:none;-webkit-user-drag:none;}' +
      '#ti89-hits{position:absolute;inset:0;}' +
      '.ti89-hit{position:absolute;cursor:pointer;border-radius:7px;-webkit-tap-highlight-color:transparent;}' +
      '.ti89-hit.pressed{box-shadow:inset 0 0 0 2px rgba(255,255,255,.85),0 0 6px 1px rgba(255,255,255,.45);background:rgba(255,255,255,.10);}' +
      '#background-left,#background-center,#background-right,#calccontainer,#textandbuttons,#fnbuttonscontainer,#keyboardcontainer,#adjustbar,#adjustbartooltip,#disclaimertooltip{display:none !important;}';
    document.head.appendChild(css);

    var root=document.createElement('div'); root.id='skinroot';
    var disp=document.createElement('div'); disp.id='ti89-display';
    var kb  =document.createElement('div'); kb.id='ti89-keyboard';
    var img =document.createElement('img'); img.id='kbimg'; img.src='img/ti89_hw1_keyboard.png'; img.alt='TI-89 keyboard';
    var hits=document.createElement('div'); hits.id='ti89-hits';
    kb.appendChild(img); kb.appendChild(hits);
    root.appendChild(disp); root.appendChild(kb);
    document.body.appendChild(root);

    var scr=document.getElementById('screen'); if(scr) disp.appendChild(scr);
    var tint=document.createElement('div'); tint.id='ti89-tint'; disp.appendChild(tint);

    KEYS.forEach(function(k){
      var sk=k[1];
      var d=document.createElement('div'); d.className='ti89-hit'; d.title=k[0];
      d.style.left=(k[2]*100)+'%'; d.style.top=(k[3]*100)+'%';
      d.style.width=((k[4]-k[2])*100)+'%'; d.style.height=((k[5]-k[3])*100)+'%';
      function press(e){ if(e&&e.cancelable) e.preventDefault(); d.classList.add('pressed');
        if(typeof emu==='undefined'||!emu) return;
        if(sk==='ON'){ if(emu.setONKeyPressed) emu.setONKeyPressed(); } else { emu.setKey(sk,1); } }
      function release(){ d.classList.remove('pressed');
        if(typeof emu==='undefined'||!emu) return;
        if(sk==='ON'){ if(emu.setONKeyReleased) emu.setONKeyReleased(); } else { emu.setKey(sk,0); } }
      d.addEventListener('mousedown',press);
      d.addEventListener('mouseup',release);
      d.addEventListener('mouseleave',release);
      d.addEventListener('touchstart',press,{passive:false});
      d.addEventListener('touchend',release);
      d.addEventListener('touchcancel',release);
      hits.appendChild(d);
    });

    function layout(){
      var vw=window.innerWidth, vh=window.innerHeight;
      // Use the emulator's actual LCD buffer aspect so the display is never
      // stretched, whatever resolution the core renders at (fallback 1.6).
      var aspect=(scr && scr.width>0 && scr.height>0) ? (scr.width/scr.height) : DISP_ASPECT;
      var dispWfrac=SPAN_R-SPAN_L;
      var perKbW=dispWfrac/aspect + GAP + KB_H/KB_W;   // total stack height per unit kb width
      var KW=Math.floor(Math.min(vw, vh/perKbW));
      var kbHpx=KW*KB_H/KB_W;
      var dW=KW*dispWfrac, dH=dW/aspect, gapPx=KW*GAP;
      kb.style.width=KW+'px'; kb.style.height=kbHpx+'px';
      disp.style.width=dW+'px'; disp.style.height=dH+'px'; disp.style.marginBottom=gapPx+'px';
      disp.style.left='0px';   // centered under the keyboard (flex handles horizontal centering)
    }
    window.__ti89skin={ ready:true, layout:layout };
    layout();
    try { attachStateMenu(); } catch (e) {}
  })();

  window.onresize = adjustComponentsSize;
  adjustComponentsSize();
  adjustComponentsSize();
  function displayElements() {
    // if (displayStyle == 1) {
    //   $('#calccontainer #calcimg').css('display', "block");
    //   $('#calccontainer #calcimg2').css('display', "none");
    // } else {
    //   $('#calccontainer #calcimg').css('display', "none");
    //   $('#calccontainer #calcimg2').css('display', "block");
    // }
    $('#calccontainer #calcimg2').css('display', "block");
    $('#keyboardcontainer').css('display', 'block');
  }
  displayElements();
  downloadV12MV12();
  setTimeout(function() {
    adjustComponentsSize();
    //$('#calccontainer #calcimg2').css('display', "block");
  }, 500);
});
