import os
import glob
import modal

stub = modal.Stub("build script")
mount = modal.Mount.from_local_dir("./", remote_path="/root/midas-rs")
image = modal.Image.from_registry("rust:latest", add_python="3.10").pip_install("maturin")


@stub.function(image=image, mounts=[mount], cpu=2)
def build():
    os.system("""
        cd /root/midas-rs
        maturin build --release --strip
    """)
    ret = {}
    wheels = glob.glob("/root/midas-rs/target/wheels/*.whl")
    for w in wheels:
        with open(w, "rb") as f:
            raw = f.read()
            ret[w.split('/')[-1]] = raw
    return ret


@stub.local_entrypoint()
def main():
    wheels = build.remote()
    for w, raw in wheels.items():
        w = os.path.join("./builds", w)
        with open(w, "wb") as f:
            f.write(raw)
