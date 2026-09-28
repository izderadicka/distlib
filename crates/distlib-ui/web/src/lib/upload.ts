// Sending one file to the node (`POST /upload`, D5), for `library.add`.
//
// `XMLHttpRequest`, not `fetch`: only it reports how much of a request body
// has been sent, and a file worth uploading can take minutes.

import { Unauthorised } from "./rpc";
import { forgetToken, token } from "./token";

/** What the node answers an upload with. */
export interface Uploaded {
  upload: string;
  filename: string;
  size: number;
}

/** The upload did not arrive, and why, as far as can be told. */
export class UploadFailed extends Error {}

/** Uploads `file`, telling `onProgress` how many of its bytes have been sent. */
export function upload(file: File, onProgress: (sent: number) => void): Promise<Uploaded> {
  const held = token();
  if (held === null) {
    return Promise.reject(new Unauthorised());
  }
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("POST", `/upload?filename=${encodeURIComponent(file.name)}`);
    request.setRequestHeader("Authorization", `Bearer ${held}`);
    request.upload.onprogress = (event) => onProgress(event.loaded);
    request.onload = () => {
      if (request.status === 401) {
        forgetToken();
        reject(new Unauthorised());
        return;
      }
      // Every answer is JSON, refusals included (D9).
      let answer: { error?: { message?: string } } & Partial<Uploaded>;
      try {
        answer = JSON.parse(request.responseText);
      } catch {
        answer = {};
      }
      if (request.status === 200 && answer.upload) {
        resolve(answer as Uploaded);
      } else {
        reject(new UploadFailed(answer.error?.message ?? `the node answered ${request.status}`));
      }
    };
    // A node that refuses a body while it is still arriving — one too large,
    // say — may close the connection before the browser has read the answer.
    request.onerror = () =>
      reject(
        new UploadFailed(
          `${file.name} was cut off before the node answered: it may be larger than the node takes, or the node may have stopped`,
        ),
      );
    request.send(file);
  });
}
