echo "nameserver 8.8.8.8" | tee /etc/resolv.conf > /dev/null && \
    chown root:root -R /etc/sudoers /etc/sudoers.d && \
    chmod 644 /usr/lib/sudo/sudoers.so && \
    chown root:root /usr/bin/sudo && chmod 4755 /usr/bin/sudo && \
    chown -R root:root /usr/lib/sudo && \
    usermod -s /bin/bash hadoop && \
    usermod -aG sudo hadoop && \
    usermod -aG root hadoop