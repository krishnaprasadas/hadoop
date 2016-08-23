package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.permission.FsPermission;

public class PosixPermissionUtils {

    public static Set<PosixFilePermission> setPosixPermission(Path workingDir,Path p, FsPermission permission) throws IOException {
        final Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
        synchronized (p) {
            switch (permission.getUserAction()) {
            case ALL:
                perms.add(PosixFilePermission.OWNER_READ);
                perms.add(PosixFilePermission.OWNER_WRITE);
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                break;
            case EXECUTE:
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                break;
            case NONE:

                break;
            case READ:
                perms.add(PosixFilePermission.OWNER_READ);
                break;
            case READ_EXECUTE:
                perms.add(PosixFilePermission.OWNER_READ);
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                break;
            case READ_WRITE:
                perms.add(PosixFilePermission.OWNER_READ);
                perms.add(PosixFilePermission.OWNER_WRITE);
                break;
            case WRITE:
                perms.add(PosixFilePermission.OWNER_WRITE);
                break;
            case WRITE_EXECUTE:
                perms.add(PosixFilePermission.OWNER_WRITE);
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                break;

            default:
                break;
            }
            switch (permission.getGroupAction()) {
            case ALL:
                perms.add(PosixFilePermission.GROUP_READ);
                perms.add(PosixFilePermission.GROUP_WRITE);
                perms.add(PosixFilePermission.GROUP_EXECUTE);
                break;
            case EXECUTE:
                perms.add(PosixFilePermission.GROUP_EXECUTE);
                break;
            case NONE:

                break;
            case READ:
                perms.add(PosixFilePermission.GROUP_READ);
                break;
            case READ_EXECUTE:
                perms.add(PosixFilePermission.GROUP_READ);
                perms.add(PosixFilePermission.GROUP_EXECUTE);
                break;
            case READ_WRITE:
                perms.add(PosixFilePermission.GROUP_READ);
                perms.add(PosixFilePermission.GROUP_WRITE);
                break;
            case WRITE:
                perms.add(PosixFilePermission.GROUP_WRITE);
                break;
            case WRITE_EXECUTE:
                perms.add(PosixFilePermission.GROUP_WRITE);
                perms.add(PosixFilePermission.GROUP_EXECUTE);
                break;

            default:
                break;
            }
            switch (permission.getOtherAction()) {
            case ALL:
                perms.add(PosixFilePermission.OTHERS_READ);
                perms.add(PosixFilePermission.OTHERS_WRITE);
                perms.add(PosixFilePermission.OTHERS_EXECUTE);
                break;
            case EXECUTE:
                perms.add(PosixFilePermission.OTHERS_EXECUTE);
                break;
            case NONE:

                break;
            case READ:
                perms.add(PosixFilePermission.OTHERS_READ);
                break;
            case READ_EXECUTE:
                perms.add(PosixFilePermission.OTHERS_READ);
                perms.add(PosixFilePermission.OTHERS_EXECUTE);
                break;
            case READ_WRITE:
                perms.add(PosixFilePermission.OTHERS_READ);
                perms.add(PosixFilePermission.OTHERS_WRITE);
                break;
            case WRITE:
                perms.add(PosixFilePermission.OTHERS_WRITE);
                break;
            case WRITE_EXECUTE:
                perms.add(PosixFilePermission.OTHERS_WRITE);
                perms.add(PosixFilePermission.OTHERS_EXECUTE);
                break;

            default:
                break;
            }
        }
        URI uri = p.toUri();
        java.nio.file.Path nioPath = null;
        try {
            uri = uri.getFragment() == null ? uri : new URI(uri.getScheme(), uri.getSchemeSpecificPart(), null);
            if (uri.getScheme() == null) {
                String path = uri.getPath();
                if (!path.startsWith("/")) {
                    nioPath = Paths.get(workingDir.toUri().getPath(), path);
                } else {
                    uri = new URI("file:///" + path);
                    nioPath = Paths.get(uri);
                }

            } else {
                nioPath = Paths.get(uri);
            }
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
        Files.setPosixFilePermissions(nioPath, perms);
        return perms;
    }
}
