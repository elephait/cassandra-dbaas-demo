package demo.platform.repair;

import demo.platform.repair.OperationRecord.RepairPolicy;
import demo.platform.repair.persistence.RepairPolicyRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
@RequiredArgsConstructor
public class RepairConfigurationService {
    private final RepairPolicyRepository policies;

    @Transactional(propagation = Propagation.MANDATORY)
    public RepairPolicy savePolicy(RepairPolicy policy) {
        return policies.save(policy);
    }

    @Transactional(readOnly = true)
    public RepairPolicy getPolicy(String clusterId) {
        return policies.get(clusterId);
    }

    @Transactional(readOnly = true)
    public Map<String, RepairPolicy> listPolicies() {
        return policies.list();
    }

}
