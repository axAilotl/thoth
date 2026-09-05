"""Restore configured LiveSync timers after headless Obsidian becomes ready.

Does not change sync settings, credentials, conflicts, or source files. Used by
the purrsephone user service's ExecStartPost. Fails visibly if readiness cannot
be verified. Invoke only after the service has started Obsidian.
"""
import json
import subprocess
import sys
import time

CODE = '''(async()=>{
const p=app.plugins.plugins["obsidian-livesync"];
if(!p?.services.appLifecycle.isReady()) return JSON.stringify({ready:false});
const m=p.modules.find(m=>m.constructor.name==="ModulePeriodicProcess");
if(p.settings.periodicReplication && !m?.periodicSyncProcessor._timer)
  await p.services.setting.realiseSetting();
return JSON.stringify({ready:true,periodic:p.settings.periodicReplication,
  timer:!!m?.periodicSyncProcessor._timer});
})()'''


def ready_from_output(output):
    for line in output.splitlines():
        if line.startswith('=> '):
            value = json.loads(line[3:])
            return value.get('ready') is True and (
                value.get('periodic') is False or value.get('timer') is True
            )
    return False


def main():
    executable = sys.argv[1] if len(sys.argv) > 1 else 'obsidian'
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                [executable, 'vault=_vault_v', 'eval', f'code={CODE}'],
                capture_output=True, text=True, timeout=10, check=True,
            )
            if ready_from_output(result.stdout):
                print('LiveSync ready; configured periodic timer verified.')
                return
        except (subprocess.SubprocessError, ValueError, OSError):
            pass
        time.sleep(2)
    raise SystemExit('LiveSync readiness/timer verification timed out; inspect Obsidian startup.')


if __name__ == '__main__':
    main()
