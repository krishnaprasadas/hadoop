package org.apache.hadoop.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.permission.FsPermission;

public class PosixPermissionUtils {

    public static Set<PosixFilePermission> setPosixPermission(Path p, FsPermission permission) throws IOException {
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
        System.out.println("---------P.toUri()"+ p.toUri());
        Files.setPosixFilePermissions(Paths.get(p.toAbsolutePath().toString()), perms);
        return perms;
    }
}
