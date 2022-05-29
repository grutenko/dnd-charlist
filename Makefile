TARGET = $(shell rustc -vV | sed -n 's|host: ||p')
PROFILE = release

EXE = $(if $(findstring windows, $(TARGET)),.exe,)
BUNDLE = $(if $(findstring windows,$(TARGET)),msi,$(if $(findstring linux,$(TARGET)),deb,$(if $(findstring darwin,$(TARGET)),dmg,.ABORT)))

.PHONY: server all

all: server
	cd client \
	&& yarn tauri build --target=$(TARGET) --bundles=$(BUNDLE) \
	&& mkdir -p -- ../dist \
	&& mv src-tauri/target/$(TARGET)/release/bundle/$(BUNDLE)/*.$(BUNDLE) ../dist

server:
	cd server \
	&& cargo build --profile=$(PROFILE) --target=$(TARGET) \
	&& cp target/$(TARGET)/$(PROFILE)/server$(EXE) dist/server-$(TARGET)$(EXE)