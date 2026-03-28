#!/bin/bash
# Build Recode GPU Server for macOS
# Run on the Mac: bash build-mac.sh
set -e

VERSION=$(grep 'const VERSION' rrp-mac/src/main.rs | head -1 | grep -oP '"\K[^"]+')
echo "Building Recode GPU Server v${VERSION} for macOS..."

export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
export PATH=/opt/homebrew/bin:/usr/local/bin:$PATH

# Build both binaries
cargo build --release -p rrp-app -p rrp-mac

# Assemble .app bundle
APP="Recode GPU Server.app"
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"

# Copy binaries
cp target/release/recode-gpu-server "$APP/Contents/MacOS/"
cp target/release/recode-remote "$APP/Contents/MacOS/"

# Copy bundled tools (ffmpeg, ffprobe, dovi_tool, etc.)
for tool in ffmpeg ffprobe dovi_tool mediainfo mkvmerge mkvextract mkvpropedit; do
    if [[ -f "bin/$tool" ]]; then
        cp "bin/$tool" "$APP/Contents/MacOS/"
    fi
done

# Info.plist
cat > "$APP/Contents/Info.plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>Recode GPU Server</string>
    <key>CFBundleDisplayName</key>
    <string>Recode GPU Server</string>
    <key>CFBundleIdentifier</key>
    <string>co.nz.douglass.recode-gpu-server</string>
    <key>CFBundleVersion</key>
    <string>${VERSION}</string>
    <key>CFBundleShortVersionString</key>
    <string>${VERSION}</string>
    <key>CFBundleExecutable</key>
    <string>recode-gpu-server</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon</string>
    <key>LSMinimumSystemVersion</key>
    <string>13.0</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>LSUIElement</key>
    <false/>
    <key>NSNetworkVolumesUsageDescription</key>
    <string>Recode needs to access FUSE-mounted files for video encoding.</string>
</dict>
</plist>
PLIST

# Copy icon if exists
if [[ -f "Contents/Resources/AppIcon.icns" ]]; then
    cp "Contents/Resources/AppIcon.icns" "$APP/Contents/Resources/"
elif [[ -f "$HOME/Desktop/Recode GPU Server.app/Contents/Resources/AppIcon.icns" ]]; then
    cp "$HOME/Desktop/Recode GPU Server.app/Contents/Resources/AppIcon.icns" "$APP/Contents/Resources/"
fi

# Ad-hoc code sign
codesign --force --deep --sign - "$APP"

# Create DMG
DMG_PATH="Recode-GPU-Server-v${VERSION}.dmg"
rm -f "$DMG_PATH"
TMP_DMG=$(mktemp -d)
cp -a "$APP" "$TMP_DMG/"
ln -s /Applications "$TMP_DMG/Applications"
hdiutil create -volname "Recode GPU Server" -srcfolder "$TMP_DMG" -ov -format UDZO "$DMG_PATH"
rm -rf "$TMP_DMG"

echo ""
echo "Built: $APP"
echo "DMG:   $DMG_PATH ($(du -h "$DMG_PATH" | awk '{print $1}'))"
echo ""
echo "Install: Open DMG, drag to Applications"
