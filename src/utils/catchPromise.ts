type CatchPromiseSuccess<T> = [undefined, T]
type CatchPromiseError = [Error]
type CatchPromiseResult<T> = CatchPromiseSuccess<T> | CatchPromiseError

/**
 * Catches a promise and returns a tuple of [error, result]
 * If the promise is resolved, the first element of the tuple is `undefined`
 * If the promise is rejected, the first element of the tuple is the `error`
 * @param promise Promise to catch
 * @returns Tuple of [error, result]
 */
export async function catchPromise<T>(promise: Promise<T>): Promise<CatchPromiseResult<T>> {
  return promise
    .then((res) => [undefined, res] as CatchPromiseSuccess<T>)
    .catch((reason) => [reason] as CatchPromiseError)
}
