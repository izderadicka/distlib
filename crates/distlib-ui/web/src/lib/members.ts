// Who is who: for pages that name members.
import { call } from "./rpc";

/** This node's own member, and every member's name by id. */
export async function whoIsWho(): Promise<{ me: string; names: Map<string, string> }> {
  const [status, group] = await Promise.all([call("node.status", null), call("group.members", null)]);
  return { me: status.member, names: new Map(group.members.map((member) => [member.member, member.name])) };
}
